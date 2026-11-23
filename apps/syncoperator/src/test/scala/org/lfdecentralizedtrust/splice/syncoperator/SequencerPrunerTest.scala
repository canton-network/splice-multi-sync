// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.{
  SequencerMemberStatus,
  SequencerPruningStatus,
}
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}
import scala.concurrent.{Future, Promise}

class SequencerPrunerTest extends AsyncWordSpec with BaseTest {

  private val now = CantonTimestamp.assertFromInstant(Instant.parse("2026-11-23T12:00:00Z"))
  private val retention = Duration.ofDays(30)
  private val target = now.minus(retention)

  private def status(
      lowerBound: CantonTimestamp = CantonTimestamp.Epoch,
      acknowledged: CantonTimestamp = now,
  ) =
    SequencerPruningStatus(
      lowerBound,
      now,
      Set(SequencerMemberStatus.Active(participant1, CantonTimestamp.Epoch, Some(acknowledged))),
    )

  private def sequencer(statuses: SequencerPruningStatus*) = {
    val connection = mock[SequencerAdminConnection]
    when(connection.getSequencerPruningStatus()(any[TraceContext]))
      .thenReturn(
        Future.successful(statuses.head),
        statuses.tail.map(Future.successful)*
      )
    connection
  }

  private def pruned(connection: SequencerAdminConnection, result: Future[String]) =
    when(connection.prune(any[CantonTimestamp])(any[TraceContext])).thenReturn(result)

  "SequencerPruner" should {

    "prune up to the retention period with reads closed" in {
      val guard = new SequencerPruningGuard()
      val connection = sequencer(status())
      val pruneCalled = Promise[Unit]()
      val pruning = Promise[String]()
      when(connection.prune(any[CantonTimestamp])(any[TraceContext])).thenAnswer {
        (_: CantonTimestamp) =>
          pruneCalled.success(())
          pruning.future
      }
      val done = new SequencerPruner(retention, guard, loggerFactory).pruneIfDue(connection)
      for {
        _ <- pruneCalled.future
        readDuringPrune <- guard.read(Future.unit).failed
        _ = pruning.success("ok")
        _ <- done
        readAfterPrune <- guard.read(Future.successful(1))
      } yield {
        verify(connection).prune(eqTo(target))(any[TraceContext])
        readDuringPrune shouldBe a[StatusRuntimeException]
        readAfterPrune shouldBe 1
      }
    }

    "prune only as far as the members acknowledged" in {
      val acknowledged = target.minus(Duration.ofDays(1))
      val connection = sequencer(status(acknowledged = acknowledged))
      pruned(connection, Future.successful("ok"))
      new SequencerPruner(retention, new SequencerPruningGuard(), loggerFactory)
        .pruneIfDue(connection)
        .map { _ =>
          verify(connection).prune(eqTo(acknowledged))(any[TraceContext])
          succeed
        }
    }

    "open reads closed at startup without pruning when nothing is older than the retention period" in {
      val guard = new SequencerPruningGuard(startClosed = true)
      val connection = sequencer(status(lowerBound = target))
      for {
        readBefore <- guard.read(Future.unit).failed
        _ <- new SequencerPruner(retention, guard, loggerFactory).pruneIfDue(connection)
        readAfter <- guard.read(Future.successful(1))
      } yield {
        verify(connection, never).prune(any[CantonTimestamp])(any[TraceContext])
        readBefore shouldBe a[StatusRuntimeException]
        readAfter shouldBe 1
      }
    }

    "keep reads closed after a failed prune, and retry the same timestamp" in {
      val guard = new SequencerPruningGuard()
      val connection = sequencer(status(), status())
      when(connection.prune(any[CantonTimestamp])(any[TraceContext]))
        .thenReturn(Future.failed(new RuntimeException("timed out")), Future.successful("ok"))
      val pruner = new SequencerPruner(retention, guard, loggerFactory)
      for {
        _ <- pruner.pruneIfDue(connection).failed
        readAfterFailure <- guard.read(Future.unit).failed
        _ <- pruner.pruneIfDue(connection)
        readAfterRetry <- guard.read(Future.successful(1))
      } yield {
        verify(connection, times(2)).prune(eqTo(target))(any[TraceContext])
        readAfterFailure shouldBe a[StatusRuntimeException]
        readAfterRetry shouldBe 1
      }
    }

    "open reads once the bound shows that a failed call's prune finished" in {
      val guard = new SequencerPruningGuard()
      val connection = sequencer(status(), status(lowerBound = target))
      pruned(connection, Future.failed(new RuntimeException("timed out")))
      val pruner = new SequencerPruner(retention, guard, loggerFactory)
      for {
        _ <- pruner.pruneIfDue(connection).failed
        _ <- pruner.pruneIfDue(connection)
        readAfter <- guard.read(Future.successful(1))
      } yield {
        verify(connection, times(1)).prune(any[CantonTimestamp])(any[TraceContext])
        readAfter shouldBe 1
      }
    }
  }
}
