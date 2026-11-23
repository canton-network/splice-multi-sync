// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Prunes the sequencer's data older than `retentionPeriod`, as far as its members have
  * acknowledged, with the traffic report's reads closed.
  */
final class SequencerPruner(
    retentionPeriod: Duration,
    guard: SequencerPruningGuard,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  // The target of a prune whose call failed. It may still run in Canton, so reads stay closed.
  private val pending = new AtomicReference[Option[CantonTimestamp]](None)

  def pruneIfDue(sequencer: SequencerAdminConnection)(implicit tc: TraceContext): Future[Unit] =
    sequencer.getSequencerPruningStatus().flatMap { status =>
      pending.get() match {
        case Some(target) if status.lowerBound >= target => Future.successful(done())
        // Pruning at the same timestamp again waits for a prune that is still running.
        case Some(target) => prune(sequencer, target)
        case None =>
          val target = status.now.minus(retentionPeriod).min(status.safePruningTimestamp)
          if (target > status.lowerBound) prune(sequencer, target) else Future.successful(done())
      }
    }

  private def prune(sequencer: SequencerAdminConnection, target: CantonTimestamp)(implicit
      tc: TraceContext
  ): Future[Unit] = {
    pending.set(Some(target))
    for {
      _ <- guard.closeReads()
      result <- sequencer.prune(target)
    } yield {
      logger.info(s"Pruned the sequencer up to $target: $result")
      done()
    }
  }

  private def done(): Unit = {
    pending.set(None)
    guard.openReads()
  }
}
