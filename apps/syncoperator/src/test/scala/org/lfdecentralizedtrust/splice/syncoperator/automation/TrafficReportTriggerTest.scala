// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.admin.api.client.data.topology.BaseResult
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.SequencerPruningStatus
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities.{namespace, participant1}
import com.digitalasset.canton.topology.OpaquePhysicalSynchronizerId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{LsuAnnouncement, TopologyChangeOp}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty
import org.lfdecentralizedtrust.splice.automation.{TriggerContext, TriggerEnabledSynchronization}
import org.lfdecentralizedtrust.splice.config.AutomationConfig
import org.lfdecentralizedtrust.splice.environment.{RetryProvider, SequencerAdminConnection}
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologyResult
import org.lfdecentralizedtrust.splice.store.StoreTestBase
import org.lfdecentralizedtrust.splice.store.db.SplicePostgresTest
import org.lfdecentralizedtrust.splice.syncoperator.store.TrafficReportStore

import java.time.{Duration, Instant}
import scala.concurrent.Future

class TrafficReportTriggerTest
    extends StoreTestBase
    with HasExecutionContext
    with SplicePostgresTest {

  private val t0 = CantonTimestamp.assertFromInstant(Instant.parse("2026-11-02T10:00:00Z"))

  private def interval(n: Long) = t0.plus(Duration.ofMinutes(10L * n))

  /** A mocked sequencer that has sequenced past exactly the times in `consumed`, where
    * participant1 had consumed the given traffic.
    */
  private def mockSequencer(
      now: CantonTimestamp,
      consumed: Map[CantonTimestamp, Long],
      pruningLowerBound: CantonTimestamp = CantonTimestamp.Epoch,
  ) = {
    val sequencer = mock[SequencerAdminConnection]
    when(sequencer.getSequencerPruningStatus()(any[TraceContext]))
      .thenReturn(Future.successful(SequencerPruningStatus(pruningLowerBound, now, Set.empty)))
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
      .thenReturn(Future.successful(dummyDomain.toPhysical))
    when(sequencer.listLsuAnnouncements(eqTo(dummyDomain))(any[TraceContext]))
      .thenReturn(Future.successful(Seq.empty))
    sequencer
  }

  /** Makes `sequencer` serve `serial` and list the upgrade from serial 0 to 1 at `upgradeTime`. */
  private def announceUpgrade(
      sequencer: SequencerAdminConnection,
      serial: NonNegativeInt,
      upgradeTime: CantonTimestamp,
  ) = {
    val psid = dummyDomain.toPhysical.copy(serial = serial)
    when(sequencer.getPhysicalSynchronizerId()(any[TraceContext]))
      .thenReturn(Future.successful(psid))
    val successorId =
      OpaquePhysicalSynchronizerId(psid.logical, NonNegativeInt.one, psid.protocolVersion.v)
    val base = BaseResult(
      TopologyStoreId.Synchronizer(dummyDomain),
      validFrom = t0.toInstant,
      validUntil = None,
      sequenced = t0.toInstant,
      TopologyChangeOp.Replace,
      TxHash(TestHash.digest("lsu-announcement")),
      PositiveInt.one,
      NonEmpty(Seq, namespace.fingerprint),
    )
    when(sequencer.listLsuAnnouncements(eqTo(dummyDomain))(any[TraceContext]))
      .thenReturn(
        Future.successful(Seq(TopologyResult(base, LsuAnnouncement(successorId, upgradeTime))))
      )
  }

  private def mkTrigger(
      store: TrafficReportStore,
      now: CantonTimestamp,
      current: SequencerAdminConnection,
      successor: Option[SequencerAdminConnection] = None,
  ) = {
    val clock = new SimClock(now, loggerFactory)
    new TrafficReportTrigger(
      TriggerContext(
        AutomationConfig(),
        clock,
        clock,
        TriggerEnabledSynchronization.Noop,
        RetryProvider(loggerFactory, timeouts, FutureSupervisor.Noop, NoOpMetricsFactory),
        loggerFactory,
        NoOpMetricsFactory,
      ),
      dummyDomain,
      current,
      successor,
      store,
    )
  }

  "TrafficReportTrigger" should {

    "store the last interval that is over on first start" in {
      val store = new TrafficReportStore(storage)
      val now = interval(2).plusSeconds(300)
      val trigger =
        mkTrigger(store, now, mockSequencer(now, Map(interval(1) -> 100, interval(2) -> 130)))
      for {
        worked <- trigger.performWorkIfAvailable()
        idle <- trigger.performWorkIfAvailable()
        stored <- store.listIntervals(dummyDomain, interval(0), interval(6))
      } yield {
        worked shouldBe true
        idle shouldBe false
        stored shouldBe Map(interval(1) -> 30L)
      }
    }

    "store nothing until the sequencer has sequenced past the end of the interval" in {
      val store = new TrafficReportStore(storage)
      val now = interval(2).plusSeconds(300)
      val trigger = mkTrigger(store, now, mockSequencer(now, Map(interval(1) -> 100)))
      for {
        worked <- trigger.performWorkIfAvailable()
        stored <- store.listIntervals(dummyDomain, interval(0), interval(6))
      } yield {
        worked shouldBe false
        stored shouldBe empty
      }
    }

    "skip the intervals the sequencer pruned, with a warning" in {
      val store = new TrafficReportStore(storage)
      val now = interval(6).plusSeconds(300)
      val trigger = mkTrigger(
        store,
        now,
        mockSequencer(
          now,
          Map(interval(4) -> 200, interval(5) -> 260),
          pruningLowerBound = interval(3).plusSeconds(300),
        ),
      )
      for {
        _ <- store.insertInterval(dummyDomain, interval(1), 30L)
        worked <- loggerFactory.assertLogs(
          trigger.performWorkIfAvailable(),
          _.warningMessage should include("pruned"),
        )
        stored <- store.listIntervals(dummyDomain, interval(0), interval(6))
      } yield {
        worked shouldBe true
        stored shouldBe Map(interval(1) -> 30L, interval(4) -> 60L)
      }
    }

    "read the interval that spans an upgrade from both sequencers" in {
      val store = new TrafficReportStore(storage)
      val now = interval(2).plusSeconds(300)
      // The current sequencer's counters freeze at the upgrade. The successor takes them over, but
      // reports zero before the upgrade.
      val current = mockSequencer(now, Map(interval(1) -> 100, interval(2) -> 100))
      val successor = mockSequencer(now, Map(interval(1) -> 0, interval(2) -> 150))
      announceUpgrade(current, NonNegativeInt.zero, upgradeTime = interval(1).plusSeconds(300))
      val trigger = mkTrigger(store, now, current, Some(successor))
      for {
        worked <- trigger.performWorkIfAvailable()
        stored <- store.listIntervals(dummyDomain, interval(0), interval(6))
      } yield {
        worked shouldBe true
        stored shouldBe Map(interval(1) -> 50L)
      }
    }

    "skip the interval that spans the upgrade once the successor is the current sequencer" in {
      val store = new TrafficReportStore(storage)
      val now = interval(3).plusSeconds(300)
      val current =
        mockSequencer(now, Map(interval(1) -> 0, interval(2) -> 150, interval(3) -> 170))
      announceUpgrade(current, NonNegativeInt.one, upgradeTime = interval(1).plusSeconds(300))
      val trigger = mkTrigger(store, now, current)
      for {
        _ <- store.insertInterval(dummyDomain, interval(0), 30L)
        worked <- loggerFactory.assertLogs(
          trigger.performWorkIfAvailable(),
          _.warningMessage should include("predates the upgrade"),
        )
        stored <- store.listIntervals(dummyDomain, interval(0), interval(6))
      } yield {
        worked shouldBe true
        stored shouldBe Map(interval(0) -> 30L, interval(2) -> 20L)
      }
    }
  }

  override protected def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[?] =
    resetAllAppTables(storage)
}
