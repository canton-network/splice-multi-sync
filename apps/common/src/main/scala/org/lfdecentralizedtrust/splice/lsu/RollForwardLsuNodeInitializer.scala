// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.lsu

import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.transaction.GrpcConnection
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Status
import org.lfdecentralizedtrust.splice.environment.{RetryFor, RetryProvider, SynchronizerNode}
import org.lfdecentralizedtrust.splice.environment.SynchronizerNode.LocalSynchronizerNodes
import org.lfdecentralizedtrust.splice.lsu.RollForwardLsuNodeInitializer.{
  ExportTimes,
  RolledForward,
}

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}

/** Performs the synchronizer node's part of a roll-forward LSU: initializes the current node from
  * the legacy node's state and transfers the traffic state.
  *
  * Callers add the steps their own node needs after that.
  */
class RollForwardLsuNodeInitializer[T <: SynchronizerNode & LsuSynchronizerNode](
    localSynchronizerNodes: LocalSynchronizerNodes[T],
    dumpPath: Path,
    newPhysicalSynchronizerSerial: NonNegativeInt,
    newPhysicalSynchronizerProtocolVersion: ProtocolVersion,
    // If unset, we assume there is an LsuAnnouncement.
    exportTimes: Option[ExportTimes],
    clock: Clock,
    val loggerFactory: NamedLoggerFactory,
    retryProvider: RetryProvider,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val legacyNode = localSynchronizerNodes.legacy.getOrElse(
    throw new IllegalArgumentException(s"Legacy node must be set for roll-forward LSU")
  )
  private val currentNode = localSynchronizerNodes.current

  private val exporter = new LsuStateExporter(
    dumpPath,
    legacyNode.sequencerAdminConnection,
    legacyNode.mediatorAdminConnection,
    loggerFactory,
  )

  private val initializer = new LsuNodeInitializer(
    localSynchronizerNodes,
    currentNode, // roll forward goes from legacy => current
    loggerFactory,
    retryProvider,
  )

  /** The connection to the current node's sequencer, for a participant's manual LSU. */
  def successorConnection: GrpcConnection = initializer.successorConnection

  def initializeCurrentNode()(implicit tc: TraceContext): Future[RolledForward] =
    for {
      sequencerInitialized <- retryProvider.getValueWithRetries(
        RetryFor.Automation,
        "sequencer_startup",
        "New sequencer has started",
        currentNode.sequencerAdminConnection.getStatus(tc).map {
          case NodeStatus.NotInitialized(_, _, _) => false
          case NodeStatus.Success(_) => true
          case NodeStatus.Failure(msg) =>
            throw Status.FAILED_PRECONDITION
              .withDescription(s"Failed to get sequencer status: $msg")
              .asRuntimeException()
        },
        logger,
      )
      mediatorInitialized <- retryProvider.getValueWithRetries(
        RetryFor.Automation,
        "mediator_startup",
        "New mediator has started",
        currentNode.mediatorAdminConnection.getStatus(tc).map {
          case NodeStatus.NotInitialized(_, _, _) => false
          case NodeStatus.Success(_) => true
          case NodeStatus.Failure(msg) =>
            throw Status.FAILED_PRECONDITION
              .withDescription(s"Failed to get mediator status: $msg")
              .asRuntimeException()
        },
        logger,
      )
      legacyPhysicalSynchronizerId <- legacyNode.sequencerAdminConnection
        .getPhysicalSynchronizerId()
      newPhysicalSynchronizerId = PhysicalSynchronizerId(
        legacyPhysicalSynchronizerId.logical,
        newPhysicalSynchronizerSerial,
        newPhysicalSynchronizerProtocolVersion,
      )
      (_, _, upgradeTime) <- exportTimes match {
        case Some(config) =>
          val resolved = (
            config.topologyExportTime.getTimestamp(),
            config.trafficExportTime.getTimestamp(),
            config.upgradeTime.map(_.getTimestamp()),
          )
          logger.info(s"Using export timestamps from config: $config, resolved: $resolved")
          Future.successful(resolved)
        case None =>
          for {
            announcements <- legacyNode.sequencerAdminConnection.listLsuAnnouncements(
              legacyPhysicalSynchronizerId.logical
            )
          } yield {
            announcements match {
              case Seq(announcement) =>
                logger.info(s"Using export timestamps from announcement: $announcement")
                (
                  CantonTimestamp.assertFromInstant(announcement.base.validFrom),
                  announcement.mapping.upgradeTime,
                  Some(announcement.mapping.upgradeTime),
                )
              case _ =>
                throw new IllegalStateException(
                  s"Expected exactly one LSU announcement but got: $announcements"
                )
            }
          }

      }
      _ <-
        if (sequencerInitialized && mediatorInitialized) {
          logger.info("Sequencer and mediator are already initialized")
          Future.unit
        } else {
          for {
            state <- exporter.exportLSUState(
              topologyExportTime = exportTimes.map(_.topologyExportTime.getTimestamp())
            )
            _ <- initializer.initializeSynchronizer(
              state,
              newPhysicalSynchronizerId,
              now = clock.now,
              // Upgrade time is used to publish the sequencer sucessor which we don't care about for roll-forward LSUs.
              upgradeTime = None,
              ignorePsidCheck = true,
            )
            trafficState <- legacyNode.sequencerAdminConnection.getLsuTrafficControlState(ts =
              exportTimes.map(_.trafficExportTime.getTimestamp())
            )
            _ <- currentNode.sequencerAdminConnection.setLsuTrafficControlState(trafficState)
          } yield ()
        }
    } yield RolledForward(legacyPhysicalSynchronizerId, newPhysicalSynchronizerId, upgradeTime)
}

object RollForwardLsuNodeInitializer {

  /** The times at which the legacy node's state is exported. */
  final case class ExportTimes(
      topologyExportTime: LsuRollForwardTimestamp,
      trafficExportTime: LsuRollForwardTimestamp,
      // Returned in RolledForward, for callers that migrate a participant.
      upgradeTime: Option[LsuRollForwardTimestamp],
  )

  final case class RolledForward(
      legacyPhysicalSynchronizerId: PhysicalSynchronizerId,
      newPhysicalSynchronizerId: PhysicalSynchronizerId,
      // The upgrade time for a participant's manual LSU. None uses its ledger end.
      upgradeTime: Option[CantonTimestamp],
  )
}
