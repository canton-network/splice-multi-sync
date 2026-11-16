// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.topology.BaseResult
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.SequencerPruningStatus
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  namespace,
  participant1,
  synchronizerId,
}
import com.digitalasset.canton.topology.OpaquePhysicalSynchronizerId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{LsuAnnouncement, TopologyChangeOp}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologyResult
import org.lfdecentralizedtrust.splice.http.v0.definitions.TrafficReportInterval.Status
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}
import scala.concurrent.Future

class TrafficReportReaderTest extends AsyncWordSpec with BaseTest {

  private val t0 = CantonTimestamp.assertFromInstant(Instant.parse("2026-11-02T10:00:00Z"))

  private def interval(n: Long) = t0.plus(Duration.ofMinutes(10L * n))

  /** A mocked sequencer that has sequenced past exactly the times in `consumed`, where
    * participant1 had consumed the given traffic.
    */
  private def mockSequencer(
      consumed: Map[CantonTimestamp, Long],
      pruningLowerBound: CantonTimestamp = CantonTimestamp.Epoch,
  ) = {
    val sequencer = mock[SequencerAdminConnection]
    when(sequencer.getSequencerPruningStatus()(any[TraceContext]))
      .thenReturn(
        Future.successful(SequencerPruningStatus(pruningLowerBound, interval(10), Set.empty))
      )
    when(sequencer.getLsuTrafficControlState(any[Option[CantonTimestamp]])(any[TraceContext]))
      .thenAnswer { (time: Option[CantonTimestamp]) =>
        val t = time.value
        consumed.get(t) match {
          case Some(bytes) =>
            Future.successful(
              LsuTrafficState(
                Map(
                  participant1 -> TrafficState.empty
                    .copy(extraTrafficConsumed = NonNegativeLong.tryCreate(bytes))
                )
              )(
                LsuTrafficState.protocolVersionRepresentativeFor(testedProtocolVersion)
              ).toByteString
            )
          case None =>
            Future.failed(
              ErrorCode.asGrpcError(SequencerError.NotAtUpgradeTimeOrBeyond.Error(t, None))
            )
        }
      }
    when(sequencer.getPhysicalSynchronizerId()(any[TraceContext]))
      .thenReturn(Future.successful(synchronizerId.toPhysical))
    when(sequencer.listLsuAnnouncements(eqTo(synchronizerId))(any[TraceContext]))
      .thenReturn(Future.successful(Seq.empty))
    sequencer
  }

  /** Makes `sequencer` serve `serial` and list the upgrade from serial 0 to 1 at `upgradeTime`. */
  private def announceUpgrade(
      sequencer: SequencerAdminConnection,
      serial: NonNegativeInt,
      upgradeTime: CantonTimestamp,
  ) = {
    val psid = synchronizerId.toPhysical.copy(serial = serial)
    when(sequencer.getPhysicalSynchronizerId()(any[TraceContext]))
      .thenReturn(Future.successful(psid))
    val successorId =
      OpaquePhysicalSynchronizerId(psid.logical, NonNegativeInt.one, psid.protocolVersion.v)
    val base = BaseResult(
      TopologyStoreId.Synchronizer(synchronizerId),
      validFrom = t0.toInstant,
      validUntil = None,
      sequenced = t0.toInstant,
      TopologyChangeOp.Replace,
      TxHash(TestHash.digest("lsu-announcement")),
      PositiveInt.one,
      NonEmpty(Seq, namespace.fingerprint),
    )
    when(sequencer.listLsuAnnouncements(eqTo(synchronizerId))(any[TraceContext]))
      .thenReturn(
        Future.successful(Seq(TopologyResult(base, LsuAnnouncement(successorId, upgradeTime))))
      )
  }

  private def read(
      starts: Seq[CantonTimestamp],
      current: SequencerAdminConnection,
      successor: Option[SequencerAdminConnection] = None,
  ) =
    new TrafficReportReader(
      TrafficReport(Duration.ofMinutes(10)),
      synchronizerId,
      current,
      successor,
    ).read(starts)
      .map(_.map { i =>
        (i.startRecordTime.toInstant, i.endRecordTime.toInstant, i.status, i.trafficConsumed)
      })

  private def ok(n: Long, trafficConsumed: Long) =
    (interval(n).toInstant, interval(n + 1).toInstant, Status.Ok, Some(trafficConsumed))

  private def undetermined(n: Long) =
    (interval(n).toInstant, interval(n + 1).toInstant, Status.Undetermined, None)

  private def cannotProvide(n: Long) =
    (interval(n).toInstant, interval(n + 1).toInstant, Status.CannotProvide, None)

  "TrafficReportReader" should {

    "report Undetermined from the first interval the sequencer has not sequenced past" in {
      val sequencer =
        mockSequencer(Map(interval(0) -> 100, interval(1) -> 130, interval(2) -> 150))
      read((0L until 4L).map(interval), sequencer)
        .map(_ shouldBe Seq(ok(0, 30), ok(1, 20), undetermined(2), undetermined(3)))
    }

    "report the intervals the sequencer pruned as CannotProvide" in {
      val sequencer = mockSequencer(
        Map(interval(2) -> 200, interval(3) -> 260),
        pruningLowerBound = interval(1).plusSeconds(300),
      )
      read(Seq(interval(0), interval(1), interval(2)), sequencer)
        .map(_ shouldBe Seq(cannotProvide(0), cannotProvide(1), ok(2, 60)))
    }

    "read the interval that spans an upgrade from both sequencers" in {
      // The current sequencer's counters freeze at the upgrade. The successor takes them over, but
      // reports zero before the upgrade.
      val current = mockSequencer(Map(interval(1) -> 100, interval(2) -> 100))
      val successor = mockSequencer(Map(interval(1) -> 0, interval(2) -> 150))
      announceUpgrade(current, NonNegativeInt.zero, upgradeTime = interval(1).plusSeconds(300))
      read(Seq(interval(1)), current, Some(successor))
        .map(_ shouldBe Seq(ok(1, 50)))
    }

    "report the interval that spans the upgrade as CannotProvide once the successor is the current sequencer" in {
      val current =
        mockSequencer(Map(interval(1) -> 0, interval(2) -> 150, interval(3) -> 170))
      announceUpgrade(current, NonNegativeInt.one, upgradeTime = interval(1).plusSeconds(300))
      read(Seq(interval(1), interval(2)), current)
        .map(_ shouldBe Seq(cannotProvide(1), ok(2, 20)))
    }
  }
}
