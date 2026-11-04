// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.automation

import cats.implicits.catsSyntaxTuple2Semigroupal
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.GrantUnlimitedTrafficTriggerBase.{
  Task,
  UnlimitedTraffic,
}
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot

import scala.concurrent.{ExecutionContext, Future}

/** Grants unlimited traffic on a sequencer to members that cannot buy their own, such as the
  * mediators and the SV participants of a synchronizer. Subclasses decide which members those are.
  */
abstract class GrantUnlimitedTrafficTriggerBase(
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration
)(implicit
    ec: ExecutionContext,
    mat: Materializer,
    tracer: Tracer,
) extends PollingParallelTaskExecutionTrigger[Task] {

  /** Admin connection to the sequencer this trigger grants on. */
  protected def sequencerAdminConnection()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection]

  /** Whether the task's member is still one this trigger grants for. */
  protected def isActiveMember(task: Task)(implicit tc: TraceContext): Future[Boolean]

  override protected final def completeTask(task: Task)(implicit
      tc: TraceContext
  ): Future[TaskOutcome] =
    for {
      connection <- sequencerAdminConnection()
      // We must read the state here again to pick up on new serials
      (trafficState, sequencerState) <- (
        connection.getSequencerTrafficControlState(task.memberId),
        connection.getSequencerSynchronizerState(TopologySnapshot.Sequenced),
      ).tupled
      _ <- connection.setSequencerTrafficControlState(
        trafficState,
        sequencerState,
        UnlimitedTraffic,
        context.pollingClock,
        trafficBalanceReconciliationDelay,
      )
    } yield TaskSuccess(
      s"Updated traffic limit for ${task.memberId} to NonNegativeLong.maxValue"
    )

  override protected final def isStaleTask(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] =
    for {
      connection <- sequencerAdminConnection()
      active <- isActiveMember(task)
      trafficStateO <- connection.lookupSequencerTrafficControlState(task.memberId)
    } yield {
      // A member without a traffic state has nothing to grant on, so drop the task and let the
      // next poll pick it up once the state exists.
      !active || trafficStateO.forall(_.extraTrafficLimit == UnlimitedTraffic)
    }
}

object GrantUnlimitedTrafficTriggerBase {

  val UnlimitedTraffic: NonNegativeLong = NonNegativeLong.maxValue

  final case class Task(
      synchronizerId: SynchronizerId,
      memberId: Member,
  ) extends PrettyPrinting {
    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("synchronizerId", _.synchronizerId),
        param("memberId", _.memberId),
      )
  }
}
