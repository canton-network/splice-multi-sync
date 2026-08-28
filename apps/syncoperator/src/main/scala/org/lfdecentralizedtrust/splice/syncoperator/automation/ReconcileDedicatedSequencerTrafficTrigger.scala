// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{
  ReconcileSequencerLimitWithMemberTrafficTriggerBase,
  TriggerContext,
}
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore

import scala.concurrent.{ExecutionContext, Future}

/** Reconciles the traffic purchased for this operator's synchronizer with its sequencer. The store
  * only holds purchases naming that synchronizer, so the base trigger's per-contract check is a
  * safeguard rather than the filter.
  */
class ReconcileDedicatedSequencerTrafficTrigger(
    override protected val context: TriggerContext,
    store: SyncOperatorStore,
    sequencerConnection: SequencerAdminConnection,
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    ec: ExecutionContext,
    mat: Materializer,
    tracer: Tracer,
) extends ReconcileSequencerLimitWithMemberTrafficTriggerBase(
      store,
      trafficBalanceReconciliationDelay,
    ) {

  override protected def sequencerAdminConnection()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection] =
    Future.successful(sequencerConnection)

  override protected def getTotalPurchasedMemberTraffic(memberId: Member)(implicit
      tc: TraceContext
  ): Future[Long] =
    store.getTotalPurchasedMemberTraffic(memberId)

  override protected def trafficLimitOffset(memberId: Member)(implicit
      tc: TraceContext
  ): Future[Either[String, Long]] =
    // No prior consumption to carry.
    Future.successful(Right(0L))
}
