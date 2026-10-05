// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import cats.syntax.apply.*
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.{MediatorId, Member}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{Status, StatusRuntimeException}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{
  PollingParallelTaskExecutionTrigger,
  TaskFailed,
  TaskOutcome,
  TaskSuccess,
  TriggerContext,
}
import org.lfdecentralizedtrust.splice.environment.SynchronizerNodeService
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode
import org.lfdecentralizedtrust.splice.syncoperator.automation.OutageTrafficAllowanceTrigger.Task
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

/** Sets each member's traffic limit on this operator's sequencer from the outage traffic allowance
  * in this app's config, when the app starts.
  *
  * While the global synchronizer is unavailable, nothing can be bought for this synchronizer, so
  * members run only on what was bought for them before. The operator then sets
  * `outage-traffic-allowance` and restarts this app: each member with a purchase on record gets a
  * limit of exactly its purchased total plus the allowance, so it can run that many bytes past what
  * was bought for it. Once the global synchronizer is back, the operator removes the setting and
  * restarts again: each limit is set back to the purchased total, so a member that used the
  * allowance is below zero until its purchases cover what it consumed.
  *
  * The targets only move with the config, which only changes on a restart, so once a poll finds
  * every member at its target this does no more work per member. Purchases that land in the
  * meantime are granted by [[ReconcileDedicatedSequencerTrafficTrigger]], which only raises a limit
  * to the purchased total: while the allowance is set, they pay down the credit instead of adding
  * to it.
  *
  * On a synchronizer whose sequencers several operators run, a limit changes only once the
  * sequencer group's threshold of operators set the same one, so they set and remove the allowance
  * together.
  */
class OutageTrafficAllowanceTrigger(
    override protected val context: TriggerContext,
    store: SyncOperatorStore,
    synchronizerNodeService: SynchronizerNodeService[SyncOperatorSynchronizerNode],
    allowance: Option[NonNegativeLong],
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    override val ec: ExecutionContext,
    mat: Materializer,
    override val tracer: Tracer,
) extends PollingParallelTaskExecutionTrigger[Task] {

  // Set once a poll finds every member at its target, after which polls do nothing.
  private val settled = new AtomicBoolean(false)

  override protected def retrieveTasks()(implicit tc: TraceContext): Future[Seq[Task]] =
    if (settled.get()) Future.successful(Seq.empty)
    else
      for {
        connection <- synchronizerNodeService.sequencerAdminConnection()
        totals <- store.listTotalPurchasedMemberTraffic()
        // An empty filter would list every member of the synchronizer.
        states <-
          if (totals.isEmpty) Future.successful(Seq.empty)
          else connection.listSequencerTrafficControlState(totals.keys.toSeq)
      } yield {
        val limits = states.map(state => state.member -> state.extraTrafficLimit.value).toMap
        val tasks =
          OutageTrafficAllowanceTrigger.tasks(totals, limits, allowance.map(_.value))
        if (tasks.isEmpty && !settled.getAndSet(true)) {
          val target = allowance.fold("its purchased total")(a =>
            s"its purchased total plus the outage traffic allowance of ${a.value} bytes"
          )
          logger.info(s"Every member with a purchase on record is at $target")
        }
        tasks
      }

  override protected def completeTask(task: Task)(implicit tc: TraceContext): Future[TaskOutcome] =
    for {
      connection <- synchronizerNodeService.sequencerAdminConnection()
      // Read again, so the grant carries the serial the sequencer expects next.
      (trafficState, sequencerState) <- (
        connection.getSequencerTrafficControlState(task.member),
        connection.getSequencerSynchronizerState(TopologySnapshot.Effective),
      ).tupled
      limit = trafficState.extraTrafficLimit.value
      outcome <- connection
        .setSequencerTrafficControlState(
          trafficState,
          sequencerState,
          NonNegativeLong.tryCreate(task.target),
          context.pollingClock,
          trafficBalanceReconciliationDelay,
        )
        .map[TaskOutcome](_ =>
          TaskSuccess(s"Set the traffic limit of ${task.member} from $limit to ${task.target}")
        )
        .recover {
          // The grant did not land, for example because too few of the synchronizer's operators
          // sent the same one. The next poll tries again, as the targets are not all in place.
          case ex: StatusRuntimeException
              if ex.getStatus.getCode == Status.Code.DEADLINE_EXCEEDED =>
            TaskFailed(task.notApplied(limit))
        }
    } yield outcome

  override protected def isStaleTask(task: Task)(implicit tc: TraceContext): Future[Boolean] =
    for {
      connection <- synchronizerNodeService.sequencerAdminConnection()
      state <- connection.lookupSequencerTrafficControlState(task.member)
    } yield state.forall(state => task.isDone(state.extraTrafficLimit.value))
}

object OutageTrafficAllowanceTrigger {

  /** Whether the allowance applies to a member with a purchase on record. Not to a mediator: with
    * no purchase path, it is granted unlimited traffic by [[MediatorUnlimitedTrafficTrigger]],
    * which a purchase someone made for it must not lower.
    */
  def isCovered(member: Member): Boolean = member match {
    case _: MediatorId => false
    case _ => true
  }

  /** The grants that bring each member with a purchase on record to its target, in a fixed order.
    * Members without a traffic state on the sequencer are left out, as there is nothing to set yet.
    */
  def tasks(
      totals: Map[Member, Long],
      limits: Map[Member, Long],
      allowance: Option[Long],
  ): Seq[Task] =
    totals.toSeq
      .filter { case (member, _) => isCovered(member) }
      .sortBy { case (member, _) => member.toProtoPrimitive }
      .flatMap { case (member, total) =>
        limits.get(member).flatMap { limit =>
          val task = Task(member, total, allowance)
          Option.when(!task.isDone(limit))(task)
        }
      }

  final case class Task(member: Member, total: Long, allowance: Option[Long])
      extends PrettyPrinting {

    /** The purchased total, plus the allowance while it is set, capped at the largest limit there
      * is.
      */
    def target: Long =
      allowance.fold(total)(a => if (a > Long.MaxValue - total) Long.MaxValue else total + a)

    /** Whether `limit` leaves nothing to do. Exact both ways: setting the allowance raises a limit
      * and setting a smaller one lowers it again, while removing it lowers a limit the allowance
      * raised and raises one a purchase ingested during the removal left behind.
      */
    def isDone(limit: Long): Boolean = limit == target

    /** Why a limit still at `limit` matters to the operator. */
    def notApplied(limit: Long): String = allowance match {
      case Some(a) =>
        s"Could not set the traffic limit of $member to its purchased total of $total plus the " +
          s"outage traffic allowance of $a bytes: it is still $limit. On a synchronizer whose " +
          "sequencers several operators run, it changes only once enough of them set the same " +
          "allowance."
      case None =>
        val gap = limit - total
        val side = if (gap > 0) "above" else "below"
        s"Could not bring the traffic limit of $member back to its purchased total of $total: " +
          s"it is still $limit, ${math.abs(gap)} bytes $side it. On a synchronizer whose " +
          "sequencers several operators run, it changes only once enough of them have removed " +
          "the outage traffic allowance."
    }

    override def pretty: Pretty[Task] = prettyOfClass(
      param("member", _.member),
      param("total", _.total),
      paramIfDefined("allowance", _.allowance),
      param("target", _.target),
    )
  }
}
