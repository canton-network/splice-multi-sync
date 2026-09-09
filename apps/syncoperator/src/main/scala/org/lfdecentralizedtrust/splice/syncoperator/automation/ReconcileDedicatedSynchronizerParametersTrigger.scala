// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.protocol.OnboardingRestriction.{RestrictedOpen, UnrestrictedOpen}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.topology.SynchronizerId
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
import org.lfdecentralizedtrust.splice.syncoperator.automation.ReconcileDedicatedSynchronizerParametersTrigger.Task
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore

import scala.concurrent.{ExecutionContext, Future}

/** Reconciles the dynamic parameters of the dedicated synchronizer this operator serves. */
class ReconcileDedicatedSynchronizerParametersTrigger(
    override protected val context: TriggerContext,
    store: SyncOperatorStore,
    sequencerConnection: SequencerAdminConnection,
    baseTrafficAmount: NonNegativeLong,
    permissionedSynchronizer: Boolean,
)(implicit
    override val ec: ExecutionContext,
    mat: Materializer,
    override val tracer: Tracer,
) extends PollingParallelTaskExecutionTrigger[Task] {

  private val synchronizerId = store.key.synchronizerId

  // Members onboard with charged topology transactions, and traffic cannot be bought for this
  // synchronizer until the DSO has registered it.
  override protected def retrieveTasks()(implicit
      tc: TraceContext
  ): Future[Seq[Task]] =
    store.lookupRegistration().flatMap {
      case None => Future.successful(Seq.empty)
      case Some(_) =>
        isReconciled().map(
          if (_) Seq.empty
          else Seq(Task(synchronizerId, baseTrafficAmount, permissionedSynchronizer))
        )
    }

  override protected def completeTask(task: Task)(implicit
      tc: TraceContext
  ): Future[TaskOutcome] =
    sequencerConnection
      .ensureDomainParameters(task.synchronizerId, dedicatedParameters)
      .map(_ =>
        TaskSuccess(
          s"Set the base traffic amount on ${task.synchronizerId} to ${task.baseTrafficAmount}, " +
            s"permissioned ${task.permissionedSynchronizer}"
        )
      )

  override protected def isStaleTask(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] = isReconciled()

  private def isReconciled()(implicit tc: TraceContext): Future[Boolean] =
    sequencerConnection
      .getSynchronizerParametersState(synchronizerId)
      .map(state => state.mapping.parameters == dedicatedParameters(state.mapping.parameters))

  /** Turns traffic control on if it is off, and forces the base amount either way. */
  private def dedicatedParameters(
      parameters: DynamicSynchronizerParameters
  ): DynamicSynchronizerParameters =
    parameters.tryUpdate(
      trafficControlParameters = Some(
        parameters.trafficControl
          .getOrElse(TrafficControlParameters())
          .copy(maxBaseTrafficAmount = baseTrafficAmount)
      ),
      onboardingRestriction = if (permissionedSynchronizer) RestrictedOpen else UnrestrictedOpen,
    )
}

object ReconcileDedicatedSynchronizerParametersTrigger {

  final case class Task(
      synchronizerId: SynchronizerId,
      baseTrafficAmount: NonNegativeLong,
      permissionedSynchronizer: Boolean,
  ) extends PrettyPrinting {
    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("synchronizerId", _.synchronizerId),
        param("baseTrafficAmount", _.baseTrafficAmount),
        param("permissionedSynchronizer", _.permissionedSynchronizer),
      )
  }
}
