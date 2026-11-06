// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{Member, ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{GrantUnlimitedTrafficTriggerBase, TriggerContext}
import org.lfdecentralizedtrust.splice.automation.GrantUnlimitedTrafficTriggerBase.{
  Task,
  UnlimitedTraffic,
}
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection.TrafficState
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode

import scala.concurrent.{ExecutionContext, Future}

/** Grants unlimited traffic to the participants the operator has admitted to this synchronizer,
  * once they have joined. Participants without a permission get nothing.
  */
class ParticipantUnlimitedTrafficTrigger(
    override protected val context: TriggerContext,
    synchronizerId: SynchronizerId,
    synchronizerNode: SyncOperatorSynchronizerNode,
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    override val ec: ExecutionContext,
    mat: Materializer,
    override val tracer: Tracer,
) extends GrantUnlimitedTrafficTriggerBase(trafficBalanceReconciliationDelay) {
  import ParticipantUnlimitedTrafficTrigger.*

  override protected def sequencerAdminConnection()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection] =
    Future.successful(synchronizerNode.sequencerAdminConnection)

  override protected def isActiveMember(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] =
    sequencerAdminConnection()
      .flatMap(admittedParticipants(_, task.synchronizerId))
      .map(_.contains(task.memberId))

  override protected def retrieveTasks()(implicit
      tc: TraceContext
  ): Future[Seq[Task]] =
    for {
      connection <- sequencerAdminConnection()
      admitted <- admittedParticipants(connection, synchronizerId)
      trafficStates <- joinedTrafficStates(connection, admitted)
    } yield trafficStates.collect {
      case state if state.extraTrafficLimit != UnlimitedTraffic =>
        Task(synchronizerId, state.member)
    }
}

object ParticipantUnlimitedTrafficTrigger {

  private[automation] def admittedParticipants(
      connection: SequencerAdminConnection,
      synchronizerId: SynchronizerId,
  )(implicit tc: TraceContext, ec: ExecutionContext): Future[Seq[ParticipantId]] =
    connection
      .listParticipantSynchronizerPermissions(synchronizerId)
      .map(_.map(_.mapping.participantId).distinct.sortBy(_.toProtoPrimitive))

  // The sequencer only reports a traffic state for members that have joined.
  private[automation] def joinedTrafficStates(
      connection: SequencerAdminConnection,
      members: Seq[Member],
  )(implicit tc: TraceContext): Future[Seq[TrafficState]] =
    // An empty filter would list every member.
    if (members.isEmpty) Future.successful(Seq.empty)
    else connection.listSequencerTrafficControlState(members)
}
