// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.lfdecentralizedtrust.splice.automation.{
  PeriodicTaskTrigger,
  TaskOutcome,
  TaskSuccess,
  TriggerContext,
}
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore

import scala.concurrent.{ExecutionContext, Future}

/** Warns, from startup and then every `interval`, that the outage traffic allowance is set, so the
  * operator removes it once the global synchronizer is back. Only registered while it is set.
  */
class OutageTrafficAllowanceWarningTrigger(
    interval: NonNegativeFiniteDuration,
    triggerContext: TriggerContext,
    store: SyncOperatorStore,
    allowance: NonNegativeLong,
)(implicit
    override val ec: ExecutionContext,
    override val tracer: Tracer,
) extends PeriodicTaskTrigger(interval, triggerContext) {

  override def completeTask(
      task: PeriodicTaskTrigger.PeriodicTask
  )(implicit tc: TraceContext): Future[TaskOutcome] =
    store.listTotalPurchasedMemberTraffic().map { totals =>
      val members = totals.keys.count(OutageTrafficAllowanceTrigger.isCovered)
      val credit = BigInt(allowance.value) * members
      logger.warn(
        s"The outage traffic allowance of ${allowance.value} bytes is set, covering $members " +
          s"members with a purchase on record, for a total credit outstanding of up to $credit " +
          "bytes. Once the global synchronizer is reachable again, remove " +
          "outage-traffic-allowance from this app's config and restart it."
      )
      TaskSuccess(s"Warned that the outage traffic allowance covers $members members")
    }
}
