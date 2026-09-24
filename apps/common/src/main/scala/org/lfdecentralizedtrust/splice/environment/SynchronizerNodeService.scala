// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.environment

import cats.implicits.catsSyntaxApplicativeError
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.github.blemale.scaffeine.Scaffeine
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

/** Hands out whichever synchronizer node is live, so callers keep working across an upgrade.
  * Subclasses decide when the successor has taken over. The switch is sticky once it has.
  */
abstract class SynchronizerNodeServiceBase[T <: SynchronizerNode](
    val nodes: SynchronizerNode.LocalSynchronizerNodes[T],
    cacheExpiration: NonNegativeFiniteDuration,
    retryProvider: RetryProvider,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val successorActiveRef = new java.util.concurrent.atomic.AtomicReference(false)

  private val successorActiveCache =
    ScaffeineCache.buildTracedAsync[Future, Unit, Boolean](
      Scaffeine().expireAfterWrite(cacheExpiration.asFiniteApproximation),
      implicit tc =>
        _ =>
          retryProvider.getValueWithRetries(
            RetryFor.WaitingOnInitDependency,
            "successor_active",
            "whether the successor synchronizer is active",
            successorActiveUncached(),
            logger,
          ),
    )(logger, "successorActive")

  /** Whether the successor node has taken over from the current one. */
  protected def successorActiveUncached()(implicit tc: TraceContext): Future[Boolean]

  private def successorActive()(implicit tc: TraceContext): Future[Boolean] =
    if (successorActiveRef.get()) {
      Future.successful(true)
    } else {
      successorActiveCache.get(()).map { active =>
        if (active) {
          logger.info("Switching connection to successor synchronizer")
          successorActiveRef.set(active)
        }
        active
      }
    }

  def activeSynchronizerNode()(implicit tc: TraceContext): Future[T] =
    nodes.successor match {
      case None => Future.successful(nodes.current)
      case Some(successor) =>
        successorActive().map {
          if (_) {
            successor
          } else {
            nodes.current
          }
        }
    }

  def sequencerAdminConnection()(implicit tc: TraceContext): Future[SequencerAdminConnection] =
    activeSynchronizerNode().map(_.sequencerAdminConnection)
}

/** Switches over once this node's participant is registered on the successor's serial. */
class SynchronizerNodeService[T <: SynchronizerNode](
    nodes: SynchronizerNode.LocalSynchronizerNodes[T],
    participantAdminConnection: ParticipantAdminConnection,
    globalSynchronizerAlias: SynchronizerAlias,
    cacheExpiration: NonNegativeFiniteDuration,
    retryProvider: RetryProvider,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SynchronizerNodeServiceBase[T](nodes, cacheExpiration, retryProvider) {

  override protected def successorActiveUncached()(implicit tc: TraceContext): Future[Boolean] =
    nodes.successor match {
      case None => Future.successful(false)
      case Some(successor) =>
        for {
          synchronizers <- participantAdminConnection.listRegisteredSynchronizers()
          succesorInitialized <- successor.sequencerAdminConnection
            .isNodeInitialized()
            .attemptT
            .getOrElse(false)
          global = synchronizers
            .find(
              _._1.synchronizerAlias == globalSynchronizerAlias
            )
            .flatMap(_._2.toOption)
            .getOrElse(
              throw Status.NOT_FOUND
                .withDescription(
                  s"No registered synchronizer with alias $globalSynchronizerAlias that has a physical synchronizer id"
                )
                .asRuntimeException
            )
          successorPSId <-
            if (succesorInitialized)
              successor.sequencerAdminConnection.getPhysicalSynchronizerId().map(Some(_))
            else Future.successful(None)
        } yield successorPSId.map(_.serial).contains(global.serial)
    }
}
