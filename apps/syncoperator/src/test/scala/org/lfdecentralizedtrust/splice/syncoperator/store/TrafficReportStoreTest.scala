// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.store

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.tracing.TraceContext
import org.lfdecentralizedtrust.splice.store.StoreTestBase
import org.lfdecentralizedtrust.splice.store.db.SplicePostgresTest

import java.time.{Duration, Instant}
import scala.concurrent.Future

class TrafficReportStoreTest
    extends StoreTestBase
    with HasExecutionContext
    with SplicePostgresTest {

  private val t0 = CantonTimestamp.assertFromInstant(Instant.parse("2026-11-02T10:00:00Z"))

  private def interval(n: Long) = t0.plus(Duration.ofMinutes(10L * n))

  "TrafficReportStore" should {

    "keep the first total stored for an interval" in {
      val store = new TrafficReportStore(storage)
      for {
        _ <- store.insertInterval(dummyDomain, t0, 10L)
        _ <- store.insertInterval(dummyDomain, t0, 20L)
        stored <- store.listIntervals(dummyDomain, t0, interval(1))
      } yield stored shouldBe Map(t0 -> 10L)
    }

    "list a synchronizer's intervals in a range and look up its latest one" in {
      val store = new TrafficReportStore(storage)
      for {
        _ <- Future.traverse(List(0L, 1L, 2L, 3L))(n =>
          store.insertInterval(dummyDomain, interval(n), n)
        )
        _ <- store.insertInterval(dummy2Domain, interval(5), 50L)
        stored <- store.listIntervals(dummyDomain, interval(1), interval(3))
        all <- store.listIntervals(dummyDomain, interval(0), interval(6))
        latest <- store.lookupLatestIntervalStart(dummyDomain)
      } yield {
        stored shouldBe Map(interval(1) -> 1L, interval(2) -> 2L)
        all shouldBe (0L to 3L).map(n => interval(n) -> n).toMap
        latest shouldBe Some(interval(3))
      }
    }

    "find no latest interval before one is stored" in {
      val store = new TrafficReportStore(storage)
      store.lookupLatestIntervalStart(dummyDomain).map(_ shouldBe None)
    }
  }

  override protected def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[?] =
    resetAllAppTables(storage)
}
