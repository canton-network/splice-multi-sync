// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{
  ReconcileSequencerLimitWithMemberTrafficTriggerBase,
  TaskOutcome,
  TriggerContext,
}
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.MemberTraffic
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore
import org.lfdecentralizedtrust.splice.environment.SynchronizerNodeService
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode
import org.lfdecentralizedtrust.splice.util.AssignedContract

import scala.concurrent.{ExecutionContext, Future}

/** Reconciles the traffic purchased for this operator's synchronizer with its sequencer */
class ReconcileDedicatedSequencerTrafficTrigger(
    override protected val context: TriggerContext,
    store: SyncOperatorStore,
    synchronizerNodeService: SynchronizerNodeService[SyncOperatorSynchronizerNode],
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
    outageTrafficAllowance: Option[NonNegativeLong],
)(implicit
    ec: ExecutionContext,
    mat: Materializer,
    tracer: Tracer,
) extends ReconcileSequencerLimitWithMemberTrafficTriggerBase(
      store,
      trafficBalanceReconciliationDelay,
    ) {

  // The purchases on record when this app started are the ones the allowance was set over.
  private val startedAt = context.clock.now

  override protected def sequencerAdminConnection()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection] =
    synchronizerNodeService.sequencerAdminConnection()

  override protected def getTotalPurchasedMemberTraffic(memberId: Member)(implicit
      tc: TraceContext
  ): Future[Long] =
    store.getTotalPurchasedMemberTraffic(memberId)

  override protected def trafficLimitOffset(memberId: Member)(implicit
      tc: TraceContext
  ): Future[Either[String, Long]] =
    // No prior consumption to carry.
    Future.successful(Right(0L))

  override def completeTask(
      memberTraffic: AssignedContract[MemberTraffic.ContractId, MemberTraffic]
  )(implicit tc: TraceContext): Future[TaskOutcome] = {
    // Purchases are only made on the global synchronizer, so one that lands means it is back. The
    // grant below only raises a limit to the purchased total, so it pays down the member's credit.
    if (
      outageTrafficAllowance.isDefined &&
      memberTraffic.contract.createdAt.isAfter(startedAt.toInstant)
    )
      logger.warn(
        s"A traffic purchase for ${memberTraffic.payload.memberId} landed while the outage " +
          "traffic allowance is set, so the global synchronizer is reachable again. Remove " +
          "outage-traffic-allowance from this app's config and restart it."
      )
    super.completeTask(memberTraffic)
  }
}
