// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.admin.http

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.lfdecentralizedtrust.splice.admin.http.HttpErrorHandler
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.http.v0.{definitions, sync_operator_admin as v0}
import org.lfdecentralizedtrust.splice.store.KeyValueStore
import org.lfdecentralizedtrust.splice.syncoperator.store.{ScheduledLsu, SyncOperatorKeyValueStore}
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

/** Schedules and cancels this operator's logical synchronizer upgrade.
  *
  * The schedule is held in the operator's own store; the announcement trigger publishes it once the
  * topology freeze time is reached.
  */
class HttpSyncOperatorAdminHandler(
    keyValueStore: KeyValueStore,
    sequencerAdminConnection: SequencerAdminConnection,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends v0.SyncOperatorAdminHandler[TraceContext]
    with NamedLogging {

  override def scheduleLogicalSynchronizerUpgrade(
      respond: v0.SyncOperatorAdminResource.ScheduleLogicalSynchronizerUpgradeResponse.type
  )(
      body: definitions.ScheduleLogicalSynchronizerUpgradeRequest
  )(extracted: TraceContext): Future[
    v0.SyncOperatorAdminResource.ScheduleLogicalSynchronizerUpgradeResponse
  ] = {
    implicit val tc: TraceContext = extracted
    for {
      requested <- Future.fromTry(parse(body).toTry)
      psid <- sequencerAdminConnection.getPhysicalSynchronizerId()
      _ <- requireUpgrade(requested, psid.serial, psid.protocolVersion)
      _ <- requireNoOtherUpgradeScheduled(requested)
      _ <- SyncOperatorKeyValueStore.setScheduledLsu(keyValueStore, requested)
    } yield {
      logger.info(
        s"Scheduled an upgrade to serial ${requested.newPhysicalSynchronizerSerial} at " +
          s"${requested.upgradeTime}, topology frozen from ${requested.topologyFreezeTime}"
      )
      v0.SyncOperatorAdminResource.ScheduleLogicalSynchronizerUpgradeResponseOK
    }
  }

  override def cancelLogicalSynchronizerUpgrade(
      respond: v0.SyncOperatorAdminResource.CancelLogicalSynchronizerUpgradeResponse.type
  )()(extracted: TraceContext): Future[
    v0.SyncOperatorAdminResource.CancelLogicalSynchronizerUpgradeResponse
  ] = {
    implicit val tc: TraceContext = extracted
    for {
      psid <- sequencerAdminConnection.getPhysicalSynchronizerId()
      // Removed first: clearing the schedule alone would leave an announcement the upgrade
      // trigger still acts on.
      _ <- sequencerAdminConnection.removeLsuAnnouncement(psid.logical)
      _ <- SyncOperatorKeyValueStore.clearScheduledLsu(keyValueStore)
    } yield {
      logger.info(s"Cancelled the scheduled upgrade of ${psid.logical}")
      v0.SyncOperatorAdminResource.CancelLogicalSynchronizerUpgradeResponseOK
    }
  }

  private def parse(
      body: definitions.ScheduleLogicalSynchronizerUpgradeRequest
  ): Either[Throwable, ScheduledLsu] =
    for {
      freezeTime <- CantonTimestamp
        .fromInstant(body.topologyFreezeTime.toInstant)
        .left
        .map(badRequest)
      upgradeTime <- CantonTimestamp.fromInstant(body.upgradeTime.toInstant).left.map(badRequest)
      serial <- NonNegativeInt
        .create(body.newPhysicalSynchronizerSerial)
        .left
        .map(err => badRequest(err.message))
      protocolVersion <- ProtocolVersion
        .create(body.newPhysicalSynchronizerProtocolVersion)
        .left
        .map(badRequest)
    } yield ScheduledLsu(freezeTime, upgradeTime, serial, protocolVersion)

  /** The config used to reject these at startup, so they are checked here instead. */
  private def requireUpgrade(
      requested: ScheduledLsu,
      currentSerial: NonNegativeInt,
      currentProtocolVersion: ProtocolVersion,
  ): Future[Unit] =
    if (requested.newPhysicalSynchronizerSerial <= currentSerial)
      Future.failed(
        badRequest(
          s"Serial ${requested.newPhysicalSynchronizerSerial} is not ahead of the serial " +
            s"$currentSerial this synchronizer is on."
        )
      )
    else if (requested.newPhysicalSynchronizerProtocolVersion < currentProtocolVersion)
      Future.failed(
        badRequest(
          s"Protocol version ${requested.newPhysicalSynchronizerProtocolVersion} is behind the " +
            s"version $currentProtocolVersion this synchronizer runs."
        )
      )
    else if (requested.upgradeTime.isBefore(requested.topologyFreezeTime))
      Future.failed(
        badRequest(
          s"Upgrade time ${requested.upgradeTime} is before the topology freeze time " +
            s"${requested.topologyFreezeTime}."
        )
      )
    else Future.unit

  /** Once the announcement is out, changing the schedule would not move it, so a change has to be
    * cancelled first. Rescheduling the same upgrade stays idempotent.
    */
  private def requireNoOtherUpgradeScheduled(
      requested: ScheduledLsu
  )(implicit tc: TraceContext): Future[Unit] =
    SyncOperatorKeyValueStore
      .getScheduledLsu(keyValueStore)
      .value
      .flatMap {
        case Some(scheduled) if scheduled != requested =>
          Future.failed(
            Status.ALREADY_EXISTS
              .withDescription(
                s"An upgrade to serial ${scheduled.newPhysicalSynchronizerSerial} at " +
                  s"${scheduled.upgradeTime} is already scheduled. Cancel it before scheduling " +
                  "another."
              )
              .asRuntimeException()
          )
        case _ => Future.unit
      }

  private def badRequest(message: String): Throwable =
    HttpErrorHandler.badRequest(message)
}
