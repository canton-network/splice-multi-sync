// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{GrantTrafficTriggerBase, TriggerContext}
import org.lfdecentralizedtrust.splice.automation.GrantTrafficTriggerBase.Task
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore

import scala.concurrent.{ExecutionContext, Future}

/** Reconciles every member's limit on this operator's sequencer with the traffic bought for it.
  *
  * This polls rather than reacting to each `MemberTraffic` contract as it arrives, because a member
  * has no traffic state until it has joined the synchronizer: traffic bought for it before then
  * would have nothing to be granted on, and with base rate zero that member would have no allowance
  * until it bought again.
  */
class ReconcilePurchasedTrafficTrigger(
    override protected val context: TriggerContext,
    store: SyncOperatorStore,
    sequencerConnection: SequencerAdminConnection,
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    override val ec: ExecutionContext,
    mat: Materializer,
    override val tracer: Tracer,
) extends GrantTrafficTriggerBase(trafficBalanceReconciliationDelay) {

  override protected def sequencerAdminConnection()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection] =
    Future.successful(sequencerConnection)

  // A purchase is never revoked, so the member cannot stop being one we grant for.
  override protected def isActiveMember(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] =
    Future.successful(true)

  override protected def retrieveTasks()(implicit
      tc: TraceContext
  ): Future[Seq[Task]] =
    for {
      purchasedByMember <- store.getPurchasedTrafficByMember()
      // Members the sequencer does not know yet are simply absent from this, so a member that has
      // not joined costs nothing and is picked up by a later poll.
      trafficStates <- sequencerConnection.listSequencerTrafficControlState(
        purchasedByMember.keys.toSeq
      )
    } yield trafficStates.flatMap { state =>
      purchasedByMember
        .get(state.member)
        .filter(_ > state.extraTrafficLimit.value)
        .map(purchased =>
          Task(store.key.synchronizerId, state.member, NonNegativeLong.tryCreate(purchased))
        )
    }
}
