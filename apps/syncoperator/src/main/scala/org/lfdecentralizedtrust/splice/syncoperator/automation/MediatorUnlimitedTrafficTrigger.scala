// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import cats.implicits.catsSyntaxTuple2Semigroupal
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.{MediatorId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{
  PollingParallelTaskExecutionTrigger,
  TaskOutcome,
  TaskSuccess,
  TriggerContext,
}
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot
import org.lfdecentralizedtrust.splice.syncoperator.automation.MediatorUnlimitedTrafficTrigger.{
  Task,
  UnlimitedTraffic,
}

import scala.concurrent.{ExecutionContext, Future}

/** Grants unlimited traffic to this synchronizer's mediators on its sequencer. Mediators have no
  * purchase path, so once the base rate is zero they could not send verdicts otherwise.
  */
class MediatorUnlimitedTrafficTrigger(
    override protected val context: TriggerContext,
    synchronizerId: SynchronizerId,
    sequencerConnection: SequencerAdminConnection,
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    override val ec: ExecutionContext,
    mat: Materializer,
    override val tracer: Tracer,
) extends PollingParallelTaskExecutionTrigger[Task] {

  override protected def retrieveTasks()(implicit
      tc: TraceContext
  ): Future[Seq[Task]] = {
    for {
      mediatorState <- sequencerConnection.getMediatorSynchronizerState(
        synchronizerId,
        topologySnapshot = TopologySnapshot.Effective,
      )
      mediators = mediatorState.mapping.active.forgetNE
      trafficStates <- sequencerConnection.listSequencerTrafficControlState(mediators)
    } yield {
      val limitByMember = trafficStates.map(state => state.member -> state.extraTrafficLimit).toMap
      mediators.collect {
        case mediatorId if limitByMember.get(mediatorId).exists(_ != UnlimitedTraffic) =>
          Task(mediatorId)
      }
    }
  }

  override protected def completeTask(task: Task)(implicit
      tc: TraceContext
  ): Future[TaskOutcome] = {
    for {
      // We must read the state here again to pick up on new serials
      (trafficState, sequencerState) <- (
        sequencerConnection.getSequencerTrafficControlState(task.mediatorId),
        sequencerConnection.getSequencerSynchronizerState(TopologySnapshot.Sequenced),
      ).tupled
      _ <- sequencerConnection.setSequencerTrafficControlState(
        trafficState,
        sequencerState,
        UnlimitedTraffic,
        context.pollingClock,
        trafficBalanceReconciliationDelay,
      )
    } yield TaskSuccess(
      s"Updated traffic limit for ${task.mediatorId} to NonNegativeLong.maxValue"
    )
  }

  override protected def isStaleTask(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] = {
    for {
      mediatorState <- sequencerConnection.getMediatorSynchronizerState(
        synchronizerId,
        topologySnapshot = TopologySnapshot.Effective,
      )
      trafficStateO <- sequencerConnection.lookupSequencerTrafficControlState(task.mediatorId)
    } yield {
      !mediatorState.mapping.active.contains(task.mediatorId) ||
      trafficStateO.forall(_.extraTrafficLimit == UnlimitedTraffic)
    }
  }
}

object MediatorUnlimitedTrafficTrigger {

  val UnlimitedTraffic: NonNegativeLong = NonNegativeLong.maxValue

  final case class Task(
      mediatorId: MediatorId
  ) extends PrettyPrinting {
    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("mediatorId", _.mediatorId)
      )
  }
}
