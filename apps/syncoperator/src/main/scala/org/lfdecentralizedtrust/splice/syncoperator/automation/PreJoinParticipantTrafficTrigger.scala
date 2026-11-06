// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{
  PollingParallelTaskExecutionTrigger,
  TaskOutcome,
  TaskSuccess,
  TriggerContext,
}
import org.lfdecentralizedtrust.splice.automation.GrantUnlimitedTrafficTriggerBase.{
  Task,
  UnlimitedTraffic,
}
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode
import org.lfdecentralizedtrust.splice.syncoperator.automation.ParticipantUnlimitedTrafficTrigger.{
  admittedParticipants,
  joinedTrafficStates,
}

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Purchases unlimited traffic for admitted participants before they join, so they can transact
  * as soon as they have joined. Once they have joined, [[ParticipantUnlimitedTrafficTrigger]]
  * keeps their limit at the maximum.
  */
class PreJoinParticipantTrafficTrigger(
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
  private val loggedUnsupported = new AtomicBoolean(false)

  private def connection: SequencerAdminConnection = synchronizerNode.sequencerAdminConnection

  override protected def retrieveTasks()(implicit
      tc: TraceContext
  ): Future[Seq[Task]] =
    prePurchaseSupported().flatMap {
      case false => Future.successful(Seq.empty)
      case true =>
        for {
          admitted <- admittedParticipants(connection, synchronizerId)
          pending = admitted.filterNot(purchased.contains).filterNot(recentlySubmitted)
          joined <- joinedTrafficStates(connection, pending).map(_.map(_.member).toSet)
        } yield pending.filterNot(joined.contains).map(Task(synchronizerId, _))
    }

  override protected def completeTask(task: Task)(implicit
      tc: TraceContext
  ): Future[TaskOutcome] =
    connection.purchaseTrafficBeforeJoin(task.memberId, UnlimitedTraffic).map {
      case true =>
        submittedAt.put(task.memberId, context.clock.now).discard
        TaskSuccess(s"Purchased unlimited traffic for ${task.memberId} before it joined")
      case false =>
        submittedAt.remove(task.memberId).discard
        purchased.put(task.memberId, ()).discard
        TaskSuccess(s"Traffic for ${task.memberId} was already purchased before it joined")
    }

  override protected def isStaleTask(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] =
    if (purchased.contains(task.memberId)) Future.successful(true)
    else
      for {
        admitted <- admittedParticipants(connection, task.synchronizerId)
        joined <- joinedTrafficStates(connection, Seq(task.memberId))
      } yield !admitted.contains(task.memberId) || joined.nonEmpty

  // A submission is only retried once it had time to be sequenced. The retry then reports
  // whether it was.
  private def recentlySubmitted(member: Member): Boolean =
    submittedAt
      .get(member)
      .exists(
        _.plus(trafficBalanceReconciliationDelay.asJavaApproximation).isAfter(context.clock.now)
      )

  private def prePurchaseSupported()(implicit tc: TraceContext): Future[Boolean] =
    connection.getStatus.map { status =>
      val protocolVersion = status.trySuccess.protocolVersion
      val supported = protocolVersion >= ProtocolVersion.v37
      if (!supported && !loggedUnsupported.getAndSet(true))
        logger.info(
          s"Synchronizer $synchronizerId runs protocol version $protocolVersion, which cannot " +
            "take traffic purchases before a participant joins. Participants get their traffic " +
            "once they have joined."
        )
      supported
    }
}
