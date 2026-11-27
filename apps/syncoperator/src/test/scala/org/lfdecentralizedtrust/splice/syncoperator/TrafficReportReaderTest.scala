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
import com.digitalasset.canton.topology.{Member, OpaquePhysicalSynchronizerId}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{LsuAnnouncement, TopologyChangeOp}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologyResult
import org.lfdecentralizedtrust.splice.syncoperator.TrafficReportReader.{cannotProvide, notOver}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}
import scala.concurrent.Future

class TrafficReportReaderTest extends AsyncWordSpec with BaseTest {

  private val t0 = CantonTimestamp.assertFromInstant(Instant.parse("2026-11-02T10:00:00Z"))

  private def interval(n: Long) = t0.plus(Duration.ofMinutes(10L * n))

  /** A mocked sequencer that has sequenced past exactly the times in `consumed` and `left`. At the
    * times in `consumed`, participant1 had consumed the given traffic. At the times in `left`, it
    * is no longer on the synchronizer.
    */
  private def mockSequencer(
      consumed: Map[CantonTimestamp, Long],
      pruningLowerBound: CantonTimestamp = CantonTimestamp.Epoch,
      left: Set[CantonTimestamp] = Set.empty,
  ) = {
    val sequencer = mock[SequencerAdminConnection]
    when(sequencer.getSequencerPruningStatus()(any[TraceContext]))
      .thenReturn(
        Future.successful(SequencerPruningStatus(pruningLowerBound, interval(10), Set.empty))
      )
    when(sequencer.getLsuTrafficControlState(any[Option[CantonTimestamp]])(any[TraceContext]))
      .thenAnswer { (time: Option[CantonTimestamp]) =>
        val t = time.value
        val states = consumed
          .get(t)
          .map(bytes =>
            Map[Member, TrafficState](
              participant1 -> TrafficState.empty
                .copy(extraTrafficConsumed = NonNegativeLong.tryCreate(bytes))
            )
          )
          .orElse(Option.when(left.contains(t))(Map.empty[Member, TrafficState]))
        states match {
          case Some(membersTraffic) =>
            Future.successful(
              LsuTrafficState(membersTraffic)(
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
      retentionPeriod: Option[Duration] = None,
  ) =
    new TrafficReportReader(
      TrafficReport(Duration.ofMinutes(10)),
      synchronizerId,
      current,
      successor,
      retentionPeriod,
    ).read(starts)
      .map(_.map(_.map { i =>
        (i.startRecordTime.toInstant, i.endRecordTime.toInstant, i.trafficConsumed)
      }))

  private def ok(n: Long, trafficConsumed: Long) =
    (interval(n).toInstant, interval(n + 1).toInstant, trafficConsumed)

  "TrafficReportReader" should {

    "report the traffic consumed in each interval" in {
      val sequencer =
        mockSequencer(Map(interval(0) -> 100, interval(1) -> 130, interval(2) -> 150))
      read(Seq(interval(0), interval(1)), sequencer)
        .map(_ shouldBe Right(Seq(ok(0, 30), ok(1, 20))))
    }

    "reject a range with an interval the sequencer has not sequenced past" in {
      val sequencer =
        mockSequencer(Map(interval(0) -> 100, interval(1) -> 130, interval(2) -> 150))
      read((0L until 4L).map(interval), sequencer)
        .map(_ shouldBe Left(notOver(interval(2), interval(3))))
    }

    "reject a range with an interval the sequencer pruned" in {
      val sequencer = mockSequencer(
        Map(interval(2) -> 200, interval(3) -> 260),
        pruningLowerBound = interval(1).plusSeconds(300),
      )
      for {
        pruned <- read(Seq(interval(1), interval(2)), sequencer)
        afterPruning <- read(Seq(interval(2)), sequencer)
      } yield {
        pruned shouldBe Left(cannotProvide(interval(2)))
        afterPruning shouldBe Right(Seq(ok(2, 60)))
      }
    }

    "reject a range the sequencer pruned while reading it" in {
      val sequencer = mockSequencer(Map(interval(1) -> 100, interval(2) -> 130))
      when(sequencer.getSequencerPruningStatus()(any[TraceContext]))
        .thenReturn(
          Future.successful(SequencerPruningStatus(CantonTimestamp.Epoch, interval(10), Set.empty)),
          Future.successful(
            SequencerPruningStatus(interval(1).plusSeconds(300), interval(10), Set.empty)
          ),
        )
      read(Seq(interval(1)), sequencer).map(_ shouldBe Left(cannotProvide(interval(2))))
    }

    "read the interval that spans an upgrade from both sequencers" in {
      // The current sequencer's counters freeze at the upgrade. The successor takes them over, but
      // not the history before it, so its reading before the upgrade is wrong.
      val current = mockSequencer(Map(interval(1) -> 100, interval(2) -> 100))
      val successor = mockSequencer(Map(interval(1) -> 0, interval(2) -> 150))
      announceUpgrade(current, NonNegativeInt.zero, upgradeTime = interval(1).plusSeconds(300))
      read(Seq(interval(1)), current, Some(successor))
        .map(_ shouldBe Right(Seq(ok(1, 50))))
    }

    "reject a range older than the retention period" in {
      // The sequencer is at interval(10), so 85 minutes back is 5 minutes into interval(1).
      val sequencer = mockSequencer(Map(interval(1) -> 100, interval(2) -> 130, interval(3) -> 150))
      val retention = Some(Duration.ofMinutes(85))
      for {
        tooOld <- read(Seq(interval(1)), sequencer, retentionPeriod = retention)
        kept <- read(Seq(interval(2)), sequencer, retentionPeriod = retention)
      } yield {
        tooOld shouldBe Left(cannotProvide(interval(2)))
        kept shouldBe Right(Seq(ok(2, 20)))
      }
    }

    "reject a range the retention period moved past while reading it" in {
      val sequencer = mockSequencer(Map(interval(2) -> 100, interval(3) -> 130))
      when(sequencer.getSequencerPruningStatus()(any[TraceContext]))
        .thenReturn(
          Future.successful(SequencerPruningStatus(CantonTimestamp.Epoch, interval(10), Set.empty)),
          Future.successful(SequencerPruningStatus(CantonTimestamp.Epoch, interval(11), Set.empty)),
        )
      read(Seq(interval(2)), sequencer, retentionPeriod = Some(Duration.ofMinutes(85)))
        .map(_ shouldBe Left(cannotProvide(interval(3))))
    }

    "drop the traffic of a participant that left during an interval" in {
      val sequencer = mockSequencer(Map(interval(0) -> 100), left = Set(interval(1)))
      read(Seq(interval(0)), sequencer).map(_ shouldBe Right(Seq(ok(0, 0))))
    }

    "reject a range the successor pruned" in {
      val current = mockSequencer(Map(interval(1) -> 100, interval(2) -> 100))
      val successor = mockSequencer(Map(interval(2) -> 150), pruningLowerBound = interval(2))
      announceUpgrade(current, NonNegativeInt.zero, upgradeTime = interval(1).plusSeconds(300))
      read(Seq(interval(1)), current, Some(successor))
        .map(_ shouldBe Left(cannotProvide(interval(3))))
    }

    "reject the interval that spans the upgrade once the successor is the current sequencer" in {
      val current =
        mockSequencer(Map(interval(1) -> 0, interval(2) -> 150, interval(3) -> 170))
      announceUpgrade(current, NonNegativeInt.one, upgradeTime = interval(1).plusSeconds(300))
      for {
        spanning <- read(Seq(interval(1), interval(2)), current)
        afterUpgrade <- read(Seq(interval(2)), current)
      } yield {
        spanning shouldBe Left(cannotProvide(interval(2)))
        afterUpgrade shouldBe Right(Seq(ok(2, 20)))
      }
    }
  }
}
