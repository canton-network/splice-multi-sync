// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import cats.implicits.catsSyntaxApplicativeError
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import org.lfdecentralizedtrust.splice.environment.{
  RetryProvider,
  SynchronizerNode,
  SynchronizerNodeServiceBase,
}

import scala.concurrent.{ExecutionContext, Future}

/** Hands out the sequencer of whichever of this operator's synchronizer nodes is live.
  *
  * The successor is stood up well before the upgrade lands, so it counts as live only once it is
  * initialized, reports a higher serial, and the announced upgrade time has passed.
  */
class DedicatedSynchronizerNodeService(
    nodes: SynchronizerNode.LocalSynchronizerNodes[SyncOperatorSynchronizerNode],
    clock: Clock,
    cacheExpiration: NonNegativeFiniteDuration,
    retryProvider: RetryProvider,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SynchronizerNodeServiceBase[SyncOperatorSynchronizerNode](
      nodes,
      cacheExpiration,
      retryProvider,
    )
    with NamedLogging {

  override protected def successorActiveUncached()(implicit tc: TraceContext): Future[Boolean] =
    nodes.successor match {
      case None => Future.successful(false)
      case Some(successor) =>
        for {
          successorInitialized <- successor.sequencerAdminConnection
            .isNodeInitialized()
            .attemptT
            .getOrElse(false)
          active <-
            if (successorInitialized)
              for {
                currentPsid <- nodes.current.sequencerAdminConnection.getPhysicalSynchronizerId()
                successorPsid <- successor.sequencerAdminConnection.getPhysicalSynchronizerId()
                // Read from the successor, which is reachable both before and after the cut-over.
                announcements <- successor.sequencerAdminConnection
                  .listLsuAnnouncements(successorPsid.logical)
              } yield successorPsid.serial > currentPsid.serial && announcements.exists(
                announcement =>
                  announcement.mapping.successorSynchronizerId.serial == successorPsid.serial &&
                    !clock.now.isBefore(announcement.mapping.upgradeTime)
              )
            else Future.successful(false)
        } yield active
    }
}
