// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.{Member, SynchronizerId}
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
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode
import org.lfdecentralizedtrust.splice.syncoperator.automation.UnlimitedTrafficTrigger.{
  Task,
  UnlimitedTraffic,
}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Grants unlimited traffic to the mediators and the admitted participants of this synchronizer,
  * whether or not they have joined yet.
  */
class UnlimitedTrafficTrigger(
    override protected val context: TriggerContext,
    synchronizerId: SynchronizerId,
    synchronizerNode: SyncOperatorSynchronizerNode,
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    override val ec: ExecutionContext,
    mat: Materializer,
    override val tracer: Tracer,
) extends PollingParallelTaskExecutionTrigger[Task] {

  private val submittedAt = TrieMap.empty[Member, CantonTimestamp]
  private val purchased = TrieMap.empty[Member, Unit]

  private def connection: SequencerAdminConnection = synchronizerNode.sequencerAdminConnection

  override protected def retrieveTasks()(implicit
      tc: TraceContext
  ): Future[Seq[Task]] =
    members().map(
      _.filterNot(purchased.contains)
        .filterNot(recentlySubmitted)
        .map(Task(synchronizerId, _))
    )

  // Nothing else buys traffic on this synchronizer, so serial 1 is the right one for every member.
  // Buying before a member joins relies on protocol version 37, which the app checks on startup.
  override protected def completeTask(task: Task)(implicit
      tc: TraceContext
  ): Future[TaskOutcome] =
    connection.purchaseInitialTraffic(task.memberId, UnlimitedTraffic).map {
      case true =>
        submittedAt.put(task.memberId, context.clock.now).discard
        TaskSuccess(s"Purchased unlimited traffic for ${task.memberId}")
      case false =>
        submittedAt.remove(task.memberId).discard
        purchased.put(task.memberId, ()).discard
        TaskSuccess(s"Unlimited traffic for ${task.memberId} was already purchased")
    }

  override protected def isStaleTask(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] =
    if (purchased.contains(task.memberId)) Future.successful(true)
    else members().map(!_.contains(task.memberId))

  private def members()(implicit tc: TraceContext): Future[Seq[Member]] =
    for {
      permissions <- connection.listParticipantSynchronizerPermissions(synchronizerId)
      mediators <- connection.getMediatorSynchronizerState(
        synchronizerId,
        topologySnapshot = TopologySnapshot.Effective,
      )
    } yield (permissions.map(_.mapping.participantId) ++ mediators.mapping.active.forgetNE)
      .map(member => member: Member)
      .distinct
      .sortBy(_.toProtoPrimitive)

  // A submission is only retried once it had time to be sequenced. The retry then reports
  // whether it was.
  private def recentlySubmitted(member: Member): Boolean =
    submittedAt
      .get(member)
      .exists(
        _.plus(trafficBalanceReconciliationDelay.asJavaApproximation).isAfter(context.clock.now)
      )
}

object UnlimitedTrafficTrigger {

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
