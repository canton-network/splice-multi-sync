// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.{
  NodeStatus,
  SequencerAdminStatus,
  SequencerHealthStatus,
  SequencerStatus,
  TopologyQueueStatus,
}
import com.digitalasset.canton.admin.api.client.data.topology.BaseResult
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeLong,
  PositiveInt,
  PositiveLong,
}
import com.digitalasset.canton.crypto.{Fingerprint, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{SequencerSynchronizerState, TopologyChangeOp}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.{
  MediatorId,
  Member,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import io.grpc.{Status, StatusRuntimeException}
import org.lfdecentralizedtrust.splice.automation.{
  TaskNoop,
  TaskOutcome,
  TaskSuccess,
  TriggerContext,
  TriggerEnabledSynchronization,
}
import org.lfdecentralizedtrust.splice.config.AutomationConfig
import org.lfdecentralizedtrust.splice.environment.{
  RetryProvider,
  SequencerAdminConnection,
  SynchronizerNodeService,
}
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection.TrafficState
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.{
  TopologyResult,
  TopologySnapshot,
}
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode
import org.lfdecentralizedtrust.splice.syncoperator.automation.ReconcileDedicatedSequencerTrafficTrigger.{
  Task,
  tasks,
}
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.Future

class ReconcileDedicatedSequencerTrafficTriggerTest
    extends AnyWordSpec
    with BaseTest
    with HasActorSystem
    with HasExecutionContext {

  private val alice: Member = ParticipantId.tryFromProtoPrimitive("PAR::alice::default")
  private val bob: Member = ParticipantId.tryFromProtoPrimitive("PAR::bob::default")
  private val carol: Member = ParticipantId.tryFromProtoPrimitive("PAR::carol::default")
  private val mediator: Member = MediatorId(UniqueIdentifier.tryCreate("mediator", "default"))
  private val sequencer: Member = SequencerId(UniqueIdentifier.tryCreate("sequencer", "default"))

  private val allowance = Some(50L)

  // What the store holds as alice's purchased total.
  private val aliceTotal = 100L

  private val synchronizerId = SynchronizerId.tryFromString("dedicated::default")
  private val otherSynchronizerId = SynchronizerId.tryFromString("other::default")

  "ReconcileDedicatedSequencerTrafficTrigger.tasks" should {

    "grant each participant its purchased total" in {
      val result = tasks(
        limits = Map(alice -> 0L, bob -> 100L),
        totals = Map(alice -> 100L, bob -> 300L),
        allowance = None,
      )
      result shouldBe Seq(Task(alice, 100L, None), Task(bob, 300L, None))
      result.map(_.target) shouldBe Seq(100L, 300L)
    }

    "leave a participant already at its purchased total alone" in {
      tasks(
        limits = Map(alice -> 100L),
        totals = Map(alice -> 100L),
        allowance = None,
      ) shouldBe empty
    }

    "raise each participant to its purchased total plus the allowance" in {
      val result = tasks(
        limits = Map(alice -> 100L, bob -> 200L),
        totals = Map(alice -> 100L, bob -> 200L),
        allowance,
      )
      result shouldBe Seq(Task(alice, 100L, allowance), Task(bob, 200L, allowance))
      result.map(_.target) shouldBe Seq(150L, 250L)
    }

    "leave a participant already at its purchased total plus the allowance alone" in {
      tasks(
        limits = Map(alice -> 150L),
        totals = Map(alice -> 100L),
        allowance,
      ) shouldBe empty
    }

    "lower a limit a larger allowance raised to the purchased total plus a smaller one" in {
      tasks(
        limits = Map(alice -> 200L),
        totals = Map(alice -> 100L),
        allowance,
      ).map(_.target) shouldBe Seq(150L)
    }

    "lower each limit back to the purchased total once the allowance is removed" in {
      val result = tasks(
        limits = Map(alice -> 150L, bob -> 200L),
        totals = Map(alice -> 100L, bob -> 200L),
        allowance = None,
      )
      result shouldBe Seq(Task(alice, 100L, None))
      result.map(_.target) shouldBe Seq(100L)
    }

    "give a participant with no purchase on record the allowance alone" in {
      val result = tasks(
        limits = Map(carol -> 0L),
        totals = Map.empty,
        allowance,
      )
      result shouldBe Seq(Task(carol, 0L, allowance))
      result.map(_.target) shouldBe Seq(50L)
    }

    "leave a participant with no purchase on record at zero without an allowance" in {
      tasks(
        limits = Map(carol -> 0L),
        totals = Map.empty,
        allowance = None,
      ) shouldBe empty
    }

    "take back the allowance from a participant with no purchase on record once it is removed" in {
      tasks(
        limits = Map(carol -> 50L),
        totals = Map.empty,
        allowance = None,
      ).map(_.target) shouldBe Seq(0L)
    }

    "leave a mediator's unlimited traffic alone, even with a purchase on record" in {
      forEvery(Seq(allowance, None)) { allowance =>
        forEvery(Seq(0L, Long.MaxValue)) { limit =>
          tasks(
            limits = Map(mediator -> limit),
            totals = Map(mediator -> 10L),
            allowance,
          ) shouldBe empty
        }
      }
    }

    "leave a sequencer alone" in {
      tasks(
        limits = Map(sequencer -> 0L),
        totals = Map(sequencer -> 10L),
        allowance,
      ) shouldBe empty
    }

    "wait with a purchase for a member the sequencer has no traffic state for" in {
      tasks(
        limits = Map(alice -> 100L),
        totals = Map(alice -> 100L, carol -> 10L),
        allowance = None,
      ) shouldBe empty
    }

    "list the participants in a fixed order" in {
      tasks(
        limits = Map(carol -> 0L, mediator -> 0L, alice -> 0L, bob -> 0L),
        totals = Map(carol -> 1L, alice -> 1L, bob -> 1L),
        allowance,
      ).map(_.member) shouldBe Seq(alice, bob, carol)
    }
  }

  "ReconcileDedicatedSequencerTrafficTrigger.Task" should {

    "cap a target the allowance would take past the largest limit" in {
      Task(alice, 100L, Some(Long.MaxValue)).target shouldBe Long.MaxValue
      Task(alice, Long.MaxValue, allowance).target shouldBe Long.MaxValue
    }

    "leave a participant at the largest limit alone once its target is capped" in {
      tasks(
        limits = Map(alice -> Long.MaxValue),
        totals = Map(alice -> 100L),
        allowance = Some(Long.MaxValue),
      ) shouldBe empty
    }
  }

  "ReconcileDedicatedSequencerTrafficTrigger" should {

    val task = Task(alice, aliceTotal, allowance)
    // Her purchased total plus the allowance.
    val target = 150L

    "set a participant's limit to its target" in {
      val f = new Fixture(threshold = 1, limit = 0L)
      f.grantReturns(Future.successful(Status.OK))
      f.trigger.complete(task).futureValue shouldBe a[TaskSuccess]
      verify(f.connection).attemptSetSequencerTrafficControlState(
        any[TrafficState],
        any[TopologyResult[SequencerSynchronizerState]],
        eqTo(NonNegativeLong.tryCreate(target)),
        any[Clock],
        any[NonNegativeFiniteDuration],
      )(any[TraceContext])
    }

    "send no grant when the participant's current total already puts it at its target" in {
      // Retrieved before a purchase of the whole total, which has been granted since.
      val f = new Fixture(threshold = 1, limit = target)
      f.trigger.complete(task.copy(total = 0L)).futureValue shouldBe TaskNoop
      f.verifyNoGrant()
    }

    "leave a grant not applied in time to the next poll where it needs several sequencers" in {
      val f = new Fixture(threshold = 2, limit = 0L)
      f.grantReturns(Future.successful(Status.DEADLINE_EXCEEDED))
      f.trigger.complete(task).futureValue shouldBe TaskNoop
    }

    "fail a grant not applied in time where one sequencer is enough" in {
      val f = new Fixture(threshold = 1, limit = 0L)
      f.grantReturns(Future.successful(Status.DEADLINE_EXCEEDED))
      failureCode(f.trigger.complete(task)) shouldBe Status.Code.DEADLINE_EXCEEDED
    }

    "fail a grant whose sequencer call times out, even where several sequencers are needed" in {
      val f = new Fixture(threshold = 2, limit = 0L)
      f.grantReturns(Future.failed(Status.DEADLINE_EXCEEDED.asRuntimeException()))
      failureCode(f.trigger.complete(task)) shouldBe Status.Code.DEADLINE_EXCEEDED
    }

    "fail a grant a concurrent change aborted, so that it is retried" in {
      val f = new Fixture(threshold = 2, limit = 0L)
      f.grantReturns(Future.successful(Status.ABORTED))
      failureCode(f.trigger.complete(task)) shouldBe Status.Code.ABORTED
    }

    "set no limit on a sequencer that serves another synchronizer" in {
      val f = new Fixture(threshold = 1, limit = 0L, served = otherSynchronizerId)
      failureCode(f.trigger.complete(task)) shouldBe Status.Code.FAILED_PRECONDITION
      f.verifyNoGrant()
    }

    "treat a task as stale once the participant is at its target" in {
      new Fixture(threshold = 1, limit = target).trigger.stale(task).futureValue shouldBe true
    }

    "treat a task as stale once the participant's current total puts it at its target" in {
      // Retrieved before a purchase of the whole total, which has been granted since.
      new Fixture(threshold = 1, limit = target).trigger
        .stale(task.copy(total = 0L))
        .futureValue shouldBe true
    }

    "treat a task as stale once the participant has no traffic state" in {
      new Fixture(threshold = 1, limit = 0L, hasTrafficState = false).trigger
        .stale(task)
        .futureValue shouldBe true
    }

    "not treat a task as stale while the participant is not at its target" in {
      new Fixture(threshold = 1, limit = 0L).trigger.stale(task).futureValue shouldBe false
    }
  }

  private lazy val clock = new SimClock(loggerFactory = loggerFactory)

  private lazy val triggerContext = TriggerContext(
    AutomationConfig(),
    clock,
    clock,
    TriggerEnabledSynchronization.Noop,
    RetryProvider(loggerFactory, ProcessingTimeout(), FutureSupervisor.Noop, NoOpMetricsFactory),
    loggerFactory,
    NoOpMetricsFactory,
  )

  private def failureCode(outcome: Future[TaskOutcome]): Status.Code =
    inside(outcome.failed.futureValue) { case e: StatusRuntimeException => e.getStatus.getCode }

  /** The trigger on a mocked sequencer, store and node service, with alice at `limit` on a
    * sequencer group of `threshold` sequencers that serves `served`.
    */
  private class Fixture(
      threshold: Int,
      limit: Long,
      hasTrafficState: Boolean = true,
      served: SynchronizerId = synchronizerId,
  ) {
    val connection: SequencerAdminConnection = mock[SequencerAdminConnection]
    private val store = mock[SyncOperatorStore]
    private val nodeService = mock[SynchronizerNodeService[SyncOperatorSynchronizerNode]]

    when(nodeService.sequencerAdminConnection()(any[TraceContext]))
      .thenReturn(Future.successful(connection))
    when(connection.getStatus(any[TraceContext]))
      .thenReturn(Future.successful(sequencerStatus(served)))
    when(store.key).thenReturn(
      SyncOperatorStore.Key(
        PartyId.tryFromProtoPrimitive("operator::default"),
        PartyId.tryFromProtoPrimitive("dso::default"),
        synchronizerId,
      )
    )
    when(store.getTotalPurchasedMemberTraffic(eqTo(alice))(any[TraceContext]))
      .thenReturn(Future.successful(aliceTotal))
    private val trafficState = TrafficState(
      alice,
      protocol.TrafficState
        .empty(CantonTimestamp.Epoch)
        .copy(
          extraTrafficPurchased = NonNegativeLong.tryCreate(limit),
          serial = Some(PositiveInt.one),
        ),
    )
    when(connection.getSequencerTrafficControlState(eqTo(alice))(any[TraceContext]))
      .thenReturn(
        if (hasTrafficState) Future.successful(trafficState)
        else Future.failed(Status.NOT_FOUND.asRuntimeException())
      )
    when(connection.lookupSequencerTrafficControlState(eqTo(alice))(any[TraceContext]))
      .thenReturn(Future.successful(Option.when(hasTrafficState)(trafficState)))
    when(connection.getSequencerSynchronizerState(any[TopologySnapshot])(any[TraceContext]))
      .thenReturn(Future.successful(sequencerState(threshold)))

    val trigger = new TestTrigger(store, nodeService)

    def grantReturns(status: Future[Status]): Unit =
      when(
        connection.attemptSetSequencerTrafficControlState(
          any[TrafficState],
          any[TopologyResult[SequencerSynchronizerState]],
          any[NonNegativeLong],
          any[Clock],
          any[NonNegativeFiniteDuration],
        )(any[TraceContext])
      ).thenReturn(status)

    def verifyNoGrant(): Unit =
      verify(connection, never).attemptSetSequencerTrafficControlState(
        any[TrafficState],
        any[TopologyResult[SequencerSynchronizerState]],
        any[NonNegativeLong],
        any[Clock],
        any[NonNegativeFiniteDuration],
      )(any[TraceContext])
  }

  private class TestTrigger(
      store: SyncOperatorStore,
      nodeService: SynchronizerNodeService[SyncOperatorSynchronizerNode],
  ) extends ReconcileDedicatedSequencerTrafficTrigger(
        triggerContext,
        store,
        nodeService,
        NonNegativeFiniteDuration.ofSeconds(10),
        allowance.map(PositiveLong.tryCreate),
      ) {
    def complete(task: Task): Future[TaskOutcome] = completeTask(task)
    def stale(task: Task): Future[Boolean] = isStaleTask(task)
  }

  private def sequencerStatus(served: SynchronizerId): NodeStatus[SequencerStatus] =
    NodeStatus.Success(
      SequencerStatus(
        uid = UniqueIdentifier.tryCreate("sequencer", "default"),
        synchronizerId =
          PhysicalSynchronizerId(served, NonNegativeInt.tryCreate(0), ProtocolVersion.latest),
        uptime = java.time.Duration.ZERO,
        ports = Map.empty,
        connectedParticipants = Seq.empty,
        connectedMediators = Seq.empty,
        sequencer = SequencerHealthStatus(isActive = true),
        topologyQueue = TopologyQueueStatus(0, 0, 0),
        admin = SequencerAdminStatus(acceptsAdminChanges = true),
        components = Seq.empty,
        version = ReleaseVersion.current,
        protocolVersion = ProtocolVersion.latest,
      )
    )

  // A group of `threshold` sequencers, all of which have to send a grant.
  private def sequencerState(threshold: Int): TopologyResult[SequencerSynchronizerState] =
    TopologyResult(
      BaseResult(
        storeId = TopologyStoreId.Authorized,
        validFrom = Instant.EPOCH,
        validUntil = None,
        sequenced = Instant.EPOCH,
        operation = TopologyChangeOp.Replace,
        transactionHash = TxHash(TestHash.digest("sequencer-state")),
        serial = PositiveInt.one,
        signedBy = NonEmpty(Seq, Fingerprint.tryFromString("default")),
      ),
      SequencerSynchronizerState
        .create(
          synchronizerId,
          PositiveInt.tryCreate(threshold),
          (1 to threshold).map(i => SequencerId(UniqueIdentifier.tryCreate(s"seq$i", "default"))),
          Seq.empty,
        )
        .value,
    )
}
