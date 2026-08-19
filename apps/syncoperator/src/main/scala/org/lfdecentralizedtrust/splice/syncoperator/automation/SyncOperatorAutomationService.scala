// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.time.Clock
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{
  AutomationServiceCompanion,
  SpliceAppAutomationService,
  SqlIndexInitializationTrigger,
}
import org.lfdecentralizedtrust.splice.config.{AutomationConfig, SpliceParametersConfig}
import org.lfdecentralizedtrust.splice.environment.{
  PackageVersionSupport,
  RetryProvider,
  SpliceLedgerClient,
}
import org.lfdecentralizedtrust.splice.store.DomainTimeSynchronization
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore

import scala.concurrent.ExecutionContextExecutor

/** Manages background automation that runs on a sync operator app.
  *
  * The traffic reconciliation trigger, which turns the ingested purchases into sequencer traffic
  * limits, is registered here.
  */
class SyncOperatorAutomationService(
    automationConfig: AutomationConfig,
    clock: Clock,
    override val store: SyncOperatorStore,
    storage: DbStorage,
    ledgerClient: SpliceLedgerClient,
    retryProvider: RetryProvider,
    params: SpliceParametersConfig,
    protected val loggerFactory: NamedLoggerFactory,
    packageVersionSupport: PackageVersionSupport,
)(implicit
    ec: ExecutionContextExecutor,
    mat: Materializer,
    tracer: Tracer,
) extends SpliceAppAutomationService(
      automationConfig,
      clock,
      // Nothing registered here depends on domain time yet. The operator's participant is connected
      // to the dedicated synchronizer, so it can be sourced from there when something does.
      DomainTimeSynchronization.Noop,
      store,
      ledgerClient,
      retryProvider,
      params,
      packageVersionSupport,
    ) {

  override def companion: SyncOperatorAutomationService.type = SyncOperatorAutomationService

  registerTrigger(
    SqlIndexInitializationTrigger(
      storage,
      triggerContext,
    )
  )
}

object SyncOperatorAutomationService extends AutomationServiceCompanion {

  override protected[this] def expectedTriggerClasses: Seq[Nothing] =
    Seq.empty
}
