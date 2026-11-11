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
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection.TrafficState
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode
import org.lfdecentralizedtrust.splice.syncoperator.automation.UnlimitedTrafficTrigger.{
  Task,
  UnlimitedTraffic,
}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Grants unlimited traffic to the mediators and the admitted participants of this synchronizer.
  * Members the sequencer reports traffic for get the maximum with their next serial. A participant
  * that has not joined yet is bought for with serial 1, which the sequencer accepts from protocol
  * version 37 (checked on startup). Once it has joined, its traffic state shows that purchase.
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
    for {
      members <- members()
      states <- trafficStates(members)
    } yield members
      .filter { member =>
        states.get(member) match {
          case Some(state) =>
            submittedAt.remove(member).discard
            purchased.remove(member).discard
            state.extraTrafficLimit != UnlimitedTraffic
          case None => !purchased.contains(member) && !recentlySubmitted(member)
        }
      }
      .map(Task(synchronizerId, _))

  override protected def completeTask(task: Task)(implicit
      tc: TraceContext
  ): Future[TaskOutcome] =
    connection.lookupSequencerTrafficControlState(task.memberId).flatMap {
      case Some(trafficState) =>
        for {
          sequencerState <- connection.getSequencerSynchronizerState(TopologySnapshot.Sequenced)
          _ <- connection.setSequencerTrafficControlState(
            trafficState,
            sequencerState,
            UnlimitedTraffic,
            context.pollingClock,
            trafficBalanceReconciliationDelay,
          )
        } yield TaskSuccess(s"Updated traffic limit for ${task.memberId} to the maximum")
      case None =>
        connection.purchaseTrafficBeforeJoin(task.memberId, UnlimitedTraffic).map {
          case true =>
            submittedAt.put(task.memberId, context.clock.now).discard
            TaskSuccess(s"Purchased unlimited traffic for ${task.memberId} before it joined")
          case false =>
            submittedAt.remove(task.memberId).discard
            purchased.put(task.memberId, ()).discard
            TaskSuccess(s"Traffic for ${task.memberId} was already purchased before it joined")
        }
    }

  override protected def isStaleTask(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] =
    for {
      members <- members()
      stateO <- connection.lookupSequencerTrafficControlState(task.memberId)
    } yield !members.contains(task.memberId) || (stateO match {
      case Some(state) => state.extraTrafficLimit == UnlimitedTraffic
      case None => purchased.contains(task.memberId)
    })

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

  // The sequencer only reports a traffic state for members it knows, so not for participants that
  // have not joined.
  private def trafficStates(
      members: Seq[Member]
  )(implicit tc: TraceContext): Future[Map[Member, TrafficState]] =
    // An empty filter would list every member.
    if (members.isEmpty) Future.successful(Map.empty)
    else
      connection
        .listSequencerTrafficControlState(members)
        .map(_.map(state => state.member -> state).toMap)

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
