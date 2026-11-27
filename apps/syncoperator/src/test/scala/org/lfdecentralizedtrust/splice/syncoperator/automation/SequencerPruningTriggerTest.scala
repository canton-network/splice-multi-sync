// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.topology.BaseResult
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.{
  SequencerMemberStatus,
  SequencerPruningStatus,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  mediatorId,
  namespace,
  participant1,
  synchronizerId,
}
import com.digitalasset.canton.topology.{Member, OpaquePhysicalSynchronizerId}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{LsuAnnouncement, TopologyChangeOp}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import com.digitalasset.nonempty.NonEmpty
import io.grpc.{Status, StatusRuntimeException}
import org.lfdecentralizedtrust.splice.automation.{TriggerContext, TriggerEnabledSynchronization}
import org.lfdecentralizedtrust.splice.config.AutomationConfig
import org.lfdecentralizedtrust.splice.environment.{
  RetryProvider,
  SequencerAdminConnection,
  SynchronizerNodeService,
}
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologyResult
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}
import scala.concurrent.Future

class SequencerPruningTriggerTest extends AsyncWordSpec with BaseTest {

  private val now = CantonTimestamp.assertFromInstant(Instant.parse("2026-11-27T12:00:00Z"))
  private val retention = NonNegativeFiniteDuration.ofDays(30)
  private val target = now.minus(retention.asJava)
  private val behindRetention = target.minus(Duration.ofDays(1))

  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val context = TriggerContext(
    AutomationConfig(),
    clock,
    clock,
    TriggerEnabledSynchronization.Noop,
    RetryProvider(loggerFactory, ProcessingTimeout(), FutureSupervisor.Noop, NoOpMetricsFactory),
    loggerFactory,
    NoOpMetricsFactory,
  )

  /** A sequencer whose members acknowledged up to the given times, until they are disabled. With
    * `upgradeTime`, it became current in an upgrade at that time.
    */
  private def sequencer(
      acknowledged: Seq[(Member, CantonTimestamp)],
      upgradeTime: Option[CantonTimestamp] = None,
  ) = {
    val connection = mock[SequencerAdminConnection]
    val disabled = scala.collection.concurrent.TrieMap.empty[Member, Unit]
    when(connection.getSequencerPruningStatus()(any[TraceContext])).thenAnswer {
      (_: TraceContext) =>
        Future.successful(
          SequencerPruningStatus(
            CantonTimestamp.Epoch,
            now,
            acknowledged.collect {
              case (member, at) if !disabled.contains(member) =>
                SequencerMemberStatus.Active(member, CantonTimestamp.Epoch, Some(at))
            }.toSet,
          )
        )
    }
    when(connection.disableMember(any[Member])(any[TraceContext])).thenAnswer { (member: Member) =>
      Future.successful(disabled.update(member, ()))
    }
    when(connection.prune(any[CantonTimestamp])(any[TraceContext]))
      .thenReturn(Future.successful("pruned"))
    val serial = if (upgradeTime.isDefined) NonNegativeInt.one else NonNegativeInt.zero
    val psid = synchronizerId.toPhysical.copy(serial = serial)
    when(connection.getPhysicalSynchronizerId()(any[TraceContext]))
      .thenReturn(Future.successful(psid))
    when(connection.listLsuAnnouncements(eqTo(synchronizerId))(any[TraceContext]))
      .thenReturn(
        Future.successful(upgradeTime.toList.map(announcement(psid.protocolVersion.v, _)))
      )
    connection
  }

  private def announcement(protocolVersion: Int, upgradeTime: CantonTimestamp) =
    TopologyResult(
      BaseResult(
        TopologyStoreId.Synchronizer(synchronizerId),
        validFrom = now.toInstant,
        validUntil = None,
        sequenced = now.toInstant,
        TopologyChangeOp.Replace,
        TxHash(TestHash.digest("lsu-announcement")),
        PositiveInt.one,
        NonEmpty(Seq, namespace.fingerprint),
      ),
      LsuAnnouncement(
        OpaquePhysicalSynchronizerId(synchronizerId, NonNegativeInt.one, protocolVersion),
        upgradeTime,
      ),
    )

  private def prune(connection: SequencerAdminConnection) = {
    val synchronizerNodeService = mock[SynchronizerNodeService[SyncOperatorSynchronizerNode]]
    when(synchronizerNodeService.sequencerAdminConnection()(any[TraceContext]))
      .thenReturn(Future.successful(connection))
    new SequencerPruningTrigger(
      context,
      synchronizerNodeService,
      synchronizerId,
      retention,
    )(executionContext, NoReportingTracerProvider.tracer).performWorkIfAvailable()
  }

  "SequencerPruningTrigger" should {

    "disable a participant that holds pruning back, but not a mediator" in {
      val laggingParticipant = sequencer(Seq(participant1 -> behindRetention, mediatorId -> now))
      val laggingMediator = sequencer(Seq(participant1 -> now, mediatorId -> behindRetention))
      for {
        _ <- prune(laggingParticipant)
        failure <- prune(laggingMediator).failed
      } yield {
        verify(laggingParticipant).disableMember(eqTo(participant1))(any[TraceContext])
        verify(laggingParticipant).prune(eqTo(target))(any[TraceContext])
        inside(failure) { case e: StatusRuntimeException =>
          e.getStatus.getCode shouldBe Status.Code.INTERNAL
        }
        verify(laggingMediator, never).disableMember(any[Member])(any[TraceContext])
        verify(laggingMediator, never).prune(any[CantonTimestamp])(any[TraceContext])
        succeed
      }
    }

    "prune a successor only once it has served for the retention period" in {
      val acknowledged = Seq(participant1 -> now)
      val recent = sequencer(acknowledged, upgradeTime = Some(now.minus(Duration.ofDays(10))))
      val old = sequencer(acknowledged, upgradeTime = Some(behindRetention))
      for {
        _ <- prune(recent)
        _ <- prune(old)
      } yield {
        verify(recent, never).prune(any[CantonTimestamp])(any[TraceContext])
        verify(old).prune(any[CantonTimestamp])(any[TraceContext])
        succeed
      }
    }
  }
}
