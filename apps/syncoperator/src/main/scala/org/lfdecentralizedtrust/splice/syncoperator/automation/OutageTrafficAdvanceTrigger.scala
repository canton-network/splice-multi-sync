// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import cats.syntax.apply.*
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.Member
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
import org.lfdecentralizedtrust.splice.syncoperator.automation.OutageTrafficAdvanceTrigger.{
  Mode,
  Task,
}
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Grants the registration's outage traffic advance while the global synchronizer is unreachable,
  * and takes it back once it is reachable again.
  *
  * While the global synchronizer is unreachable, nothing can be bought for this synchronizer and
  * this app could not see a purchase to grant it. Once `outageAdvanceDelay` passes without a fresh
  * global-synchronizer time, each member with a purchase on record gets a limit of its purchased
  * total plus the registration's `outageAdvance`, so it keeps transacting down to that floor. Once
  * fresh time returns, each limit is set back to the purchased total, so a member that used the
  * advance is below zero until its purchases cover what it consumed.
  *
  * The target is the purchased total in this app's store, which cannot change while the global
  * synchronizer is unreachable: an app restarted during an outage computes the same target, and
  * on a synchronizer whose sequencers several operators run, the apps with up-to-date stores send
  * the same grant, which lands once the sequencer group's threshold of them do.
  *
  * Outside a switch between the two modes, this does no work per member: a mode is settled once a
  * poll finds every member at its target.
  */
class OutageTrafficAdvanceTrigger(
    override protected val context: TriggerContext,
    store: SyncOperatorStore,
    sequencerConnection: SequencerAdminConnection,
    lastGlobalSynchronizerTime: () => Option[CantonTimestamp],
    outageAdvanceDelay: NonNegativeFiniteDuration,
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    override val ec: ExecutionContext,
    mat: Materializer,
    override val tracer: Tracer,
) extends PollingParallelTaskExecutionTrigger[Task] {

  private val startedAt = context.clock.now

  // The mode whose targets were all in place at the last poll, which the polls after it skip.
  private val settledMode = new AtomicReference[Option[Mode]](None)

  private def currentMode(): Option[Mode] =
    OutageTrafficAdvanceTrigger.mode(
      context.clock.now,
      lastGlobalSynchronizerTime(),
      startedAt,
      outageAdvanceDelay,
    )

  override protected def retrieveTasks()(implicit tc: TraceContext): Future[Seq[Task]] =
    currentMode() match {
      case None => Future.successful(Seq.empty)
      case Some(mode) if settledMode.get().contains(mode) => Future.successful(Seq.empty)
      case Some(mode) =>
        for {
          totals <- store.listTotalPurchasedMemberTraffic()
          advance <- advanceFor(mode)
          // An empty filter would list every member of the synchronizer.
          states <-
            if (totals.isEmpty) Future.successful(Seq.empty)
            else sequencerConnection.listSequencerTrafficControlState(totals.keys.toSeq)
        } yield {
          val limits = states.map(state => state.member -> state.extraTrafficLimit.value).toMap
          val tasks = OutageTrafficAdvanceTrigger.tasks(mode, totals, limits, advance)
          if (tasks.isEmpty && !settledMode.getAndSet(Some(mode)).contains(mode))
            logger.info(s"Every member is at its target for $mode, with an advance of $advance")
          tasks
        }
    }

  private def advanceFor(mode: Mode)(implicit tc: TraceContext): Future[Long] = mode match {
    case Mode.Normal => Future.successful(0L)
    case Mode.Advanced =>
      // No registration means there is nothing to advance, so the targets are the totals.
      store
        .lookupRegistration()
        .map(_.fold(0L)(_.payload.governanceParameters.outageAdvance.longValue()))
  }

  override protected def completeTask(task: Task)(implicit tc: TraceContext): Future[TaskOutcome] =
    for {
      // Read again, so the grant carries the serial the sequencer expects next.
      (trafficState, sequencerState) <- (
        sequencerConnection.getSequencerTrafficControlState(task.member),
        sequencerConnection.getSequencerSynchronizerState(TopologySnapshot.Effective),
      ).tupled
      _ <- sequencerConnection.setSequencerTrafficControlState(
        trafficState,
        sequencerState,
        NonNegativeLong.tryCreate(task.target),
        context.pollingClock,
        trafficBalanceReconciliationDelay,
      )
    } yield TaskSuccess(
      s"Set the traffic limit of ${task.member} from ${trafficState.extraTrafficLimit} to ${task.target} (${task.mode})"
    )

  override protected def isStaleTask(task: Task)(implicit tc: TraceContext): Future[Boolean] =
    if (!currentMode().contains(task.mode)) Future.successful(true)
    else
      sequencerConnection
        .lookupSequencerTrafficControlState(task.member)
        .map(_.forall(state => task.isDone(state.extraTrafficLimit.value)))
}

object OutageTrafficAdvanceTrigger {

  sealed trait Mode extends Product with Serializable
  object Mode {

    /** The global synchronizer is reachable: each limit is the purchased total. */
    case object Normal extends Mode

    /** The global synchronizer is unreachable: each limit is the purchased total plus the advance. */
    case object Advanced extends Mode
  }

  /** The mode for this poll, or None while it cannot be told yet: just after startup, before the
    * first global-synchronizer time and before the delay has passed without one. Waiting keeps an
    * app restarted during an outage from taking the advance back before it has seen the outage.
    */
  def mode(
      now: CantonTimestamp,
      lastGlobalSynchronizerTime: Option[CantonTimestamp],
      startedAt: CantonTimestamp,
      outageAdvanceDelay: NonNegativeFiniteDuration,
  ): Option[Mode] = {
    val since = lastGlobalSynchronizerTime.getOrElse(startedAt)
    if ((now - since).compareTo(outageAdvanceDelay.asJava) > 0) Some(Mode.Advanced)
    else lastGlobalSynchronizerTime.map(_ => Mode.Normal)
  }

  /** The grants that bring each member with a purchase on record to its target. Members without a
    * traffic state on the sequencer are left out, as there is nothing to set yet.
    */
  def tasks(
      mode: Mode,
      totals: Map[Member, Long],
      limits: Map[Member, Long],
      advance: Long,
  ): Seq[Task] =
    totals.toSeq
      .sortBy { case (member, _) => member.toProtoPrimitive }
      .flatMap { case (member, total) =>
        limits.get(member).flatMap { limit =>
          val task = mode match {
            case Mode.Advanced => Task(member, mode, total + advance)
            case Mode.Normal => Task(member, mode, total)
          }
          Option.when(!task.isDone(limit))(task)
        }
      }

  final case class Task(member: Member, mode: Mode, target: Long) extends PrettyPrinting {

    /** Whether `limit` leaves nothing to do. The advance only ever raises a limit; the take-back
      * sets it to the total either way, lowering a limit the advance raised and raising one a
      * purchase ingested during the take-back left behind.
      */
    def isDone(limit: Long): Boolean = mode match {
      case Mode.Advanced => limit >= target
      case Mode.Normal => limit == target
    }

    override def pretty: Pretty[Task] = prettyOfClass(
      param("member", _.member),
      param("mode", _.mode.toString.unquoted),
      param("target", _.target),
    )
  }
}
