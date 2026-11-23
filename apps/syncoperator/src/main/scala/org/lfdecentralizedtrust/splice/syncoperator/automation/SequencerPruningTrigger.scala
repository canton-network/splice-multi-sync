// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.lfdecentralizedtrust.splice.automation.{PollingTrigger, TriggerContext}
import org.lfdecentralizedtrust.splice.environment.SynchronizerNodeService
import org.lfdecentralizedtrust.splice.syncoperator.{SequencerPruner, SyncOperatorSynchronizerNode}

import scala.concurrent.{ExecutionContext, Future}

/** Prunes the sequencer this operator runs, once per polling interval. */
class SequencerPruningTrigger(
    override protected val context: TriggerContext,
    synchronizerNodeService: SynchronizerNodeService[SyncOperatorSynchronizerNode],
    pruner: SequencerPruner,
)(implicit
    override val ec: ExecutionContext,
    override val tracer: Tracer,
) extends PollingTrigger {

  override def performWorkIfAvailable()(implicit tc: TraceContext): Future[Boolean] =
    synchronizerNodeService
      .sequencerAdminConnection()
      .flatMap(pruner.pruneIfDue(_))
      .map(_ => false)
}
