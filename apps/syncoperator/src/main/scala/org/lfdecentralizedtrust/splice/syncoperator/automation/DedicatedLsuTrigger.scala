// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import cats.implicits.showInterpolator
import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.transaction.LsuAnnouncement
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.{
  ScheduledTaskTrigger,
  TaskOutcome,
  TaskSuccess,
  TriggerContext,
  TriggerEnabledSynchronization,
}
import org.lfdecentralizedtrust.splice.environment.{RetryProvider, StatusAdminConnection}
import org.lfdecentralizedtrust.splice.environment.SynchronizerNode.LocalSynchronizerNodes
import org.lfdecentralizedtrust.splice.lsu.{LsuNodeInitializer, LsuStateExporter}
import org.lfdecentralizedtrust.splice.syncoperator.SyncOperatorSynchronizerNode
import org.lfdecentralizedtrust.splice.syncoperator.automation.DedicatedLsuTrigger.LsuTransferTask

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}

/** Stands up the successor of this operator's synchronizer once an LSU has been announced.
  *
  * The SV's `LsuTrigger` for a single-owner synchronizer: no DSO state to reconcile, no cometbft
  * node to rotate, and members follow the sequencer successor this publishes.
  */
class DedicatedLsuTrigger(
    baseContext: TriggerContext,
    localSynchronizerNodes: LocalSynchronizerNodes[SyncOperatorSynchronizerNode],
    successorSynchronizerNode: SyncOperatorSynchronizerNode,
    dumpPath: Path,
    retryProvider: RetryProvider,
)(implicit
    ec: ExecutionContext,
    mat: Materializer,
    tracer: Tracer,
) extends ScheduledTaskTrigger[LsuTransferTask] {

  private val currentSynchronizerNode = localSynchronizerNodes.current

  private val exporter =
    new LsuStateExporter(
      dumpPath,
      currentSynchronizerNode.sequencerAdminConnection,
      currentSynchronizerNode.mediatorAdminConnection,
      loggerFactory,
    )

  private val initializer =
    new LsuNodeInitializer(
      localSynchronizerNodes,
      successorSynchronizerNode,
      loggerFactory,
      retryProvider,
    )

  override protected lazy val context: TriggerContext =
    baseContext.copy(triggerEnabledSync = TriggerEnabledSynchronization.Noop)

  protected def listReadyTasks(now: CantonTimestamp, limit: Int)(implicit
      tc: TraceContext
  ): Future[Seq[LsuTransferTask]] =
    for {
      physicalSynchronizerId <- currentSynchronizerNode.sequencerAdminConnection
        .getPhysicalSynchronizerId()
      announcements <- announcements(now, physicalSynchronizerId)
      sequencerNotInitialized <- isNodeNotInitialized(
        successorSynchronizerNode.sequencerAdminConnection,
        "sequencer",
      )
      mediatorNotInitialized <- isNodeNotInitialized(
        successorSynchronizerNode.mediatorAdminConnection,
        "mediator",
      )
    } yield announcements
      .filter(_ => sequencerNotInitialized || mediatorNotInitialized)
      .map(result => LsuTransferTask(result.mapping))

  protected def completeTask(task: ScheduledTaskTrigger.ReadyTask[LsuTransferTask])(implicit
      tc: TraceContext
  ): Future[TaskOutcome] =
    for {
      state <- exporter.exportLSUState(topologyExportTime = None)
      parameters <- initializer.initializeSynchronizer(
        state,
        task.work.announcement.successorSynchronizerId,
        task.readyAt,
        Some(task.work.announcement.upgradeTime),
        ignorePsidCheck = false,
      )
    } yield TaskSuccess(
      show"Initialized successor synchronizer with parameters $parameters"
    )

  protected def isStaleTask(task: ScheduledTaskTrigger.ReadyTask[LsuTransferTask])(implicit
      tc: TraceContext
  ): Future[Boolean] = Future.successful(false)

  private def isNodeNotInitialized[T <: StatusAdminConnection](
      adminConnection: T,
      nodeName: String,
  )(implicit tc: TraceContext): Future[Boolean] =
    adminConnection.getStatus(tc).map {
      case NodeStatus.Failure(msg) =>
        logger.error(s"Failed to get successor $nodeName status: $msg")
        false
      case NodeStatus.NotInitialized(_, _, _) => true
      case NodeStatus.Success(_) => false
    }

  private def announcements(now: CantonTimestamp, synchronizerId: PhysicalSynchronizerId)(implicit
      tc: TraceContext
  ) =
    currentSynchronizerNode.sequencerAdminConnection
      .listLsuAnnouncements(synchronizerId.logical)
      .map(_.filter { announcement =>
        announcement.base.validFrom
          .isBefore(
            now.toInstant
          ) && announcement.mapping.successorSynchronizerId.serial > synchronizerId.serial
      })
}

object DedicatedLsuTrigger {
  final case class LsuTransferTask(announcement: LsuAnnouncement) extends PrettyPrinting {
    override def pretty: Pretty[this.type] = prettyOfClass(
      param("announcement", _.announcement)
    )
  }
}
