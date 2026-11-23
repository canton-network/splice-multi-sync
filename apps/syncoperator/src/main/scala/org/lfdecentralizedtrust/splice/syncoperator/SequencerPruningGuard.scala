// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.canton.discard.Implicits.DiscardOps
import io.grpc.Status

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Keeps traffic report reads and sequencer prunes apart. Canton prunes the traffic journal before
  * it moves the pruning bound, so a read during a prune can't tell that it read pruned rows.
  */
final class SequencerPruningGuard(implicit ec: ExecutionContext) {
  import SequencerPruningGuard.State

  private val state = new AtomicReference(State(reads = 0, closed = None))

  /** Runs `body`, or fails with UNAVAILABLE while reads are closed. */
  def read[T](body: => Future[T]): Future[T] = {
    val before = state.getAndUpdate(s => if (s.closed.isEmpty) s.copy(reads = s.reads + 1) else s)
    if (before.closed.nonEmpty)
      Future.failed(
        Status.UNAVAILABLE
          .withDescription("The sync operator app is pruning the sequencer. Retry shortly.")
          .asRuntimeException()
      )
    else Future.delegate(body).andThen(_ => endRead())
  }

  /** Stops new reads, and completes once the reads in flight are done. */
  def closeReads(): Future[Unit] = {
    val readsDone = Promise[Unit]()
    val after =
      state.updateAndGet(s => if (s.closed.isEmpty) s.copy(closed = Some(readsDone)) else s)
    if (after.reads == 0) after.closed.foreach(_.trySuccess(()).discard)
    after.closed.fold(Future.unit)(_.future)
  }

  def openReads(): Unit = state.updateAndGet(_.copy(closed = None)).discard

  private def endRead(): Unit = {
    val after = state.updateAndGet(s => s.copy(reads = s.reads - 1))
    if (after.reads == 0) after.closed.foreach(_.trySuccess(()).discard)
  }
}

object SequencerPruningGuard {
  private final case class State(reads: Int, closed: Option[Promise[Unit]])
}
