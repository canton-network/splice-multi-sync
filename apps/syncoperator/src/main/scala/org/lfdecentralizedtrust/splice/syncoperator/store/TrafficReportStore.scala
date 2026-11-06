// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import org.lfdecentralizedtrust.splice.util.FutureUnlessShutdownUtil.futureUnlessShutdownToFuture
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton

import scala.concurrent.{ExecutionContext, Future}

/** The traffic charged to participants per interval of the traffic report, keyed by the logical
  * synchronizer id.
  */
class TrafficReportStore(storage: DbStorage)(implicit
    ec: ExecutionContext,
    closeContext: CloseContext,
) {

  /** Stores the total of the interval that starts at `start`, unless it is already stored. */
  def insertInterval(
      synchronizerId: SynchronizerId,
      start: CantonTimestamp,
      trafficConsumed: Long,
  )(implicit tc: TraceContext): Future[Unit] =
    storage
      .update(
        sql"""insert into sync_operator_traffic_report
                (synchronizer_id, interval_start, traffic_consumed)
              values (${synchronizerId.toProtoPrimitive}, ${start.toMicros}, $trafficConsumed)
              on conflict do nothing""".asUpdate,
        "insertTrafficReportInterval",
      )
      .map(_.discard)

  def lookupLatestIntervalStart(
      synchronizerId: SynchronizerId
  )(implicit tc: TraceContext): Future[Option[CantonTimestamp]] =
    storage
      .query(
        sql"""select max(interval_start) from sync_operator_traffic_report
              where synchronizer_id = ${synchronizerId.toProtoPrimitive}"""
          .as[Option[Long]]
          .headOption,
        "lookupLatestTrafficReportInterval",
      )
      .map(_.flatten.map(CantonTimestamp.assertFromLong))

  /** The stored totals of the intervals that start in `[from, until)`. */
  def listIntervals(
      synchronizerId: SynchronizerId,
      from: CantonTimestamp,
      until: CantonTimestamp,
  )(implicit tc: TraceContext): Future[Map[CantonTimestamp, Long]] =
    storage
      .query(
        sql"""select interval_start, traffic_consumed from sync_operator_traffic_report
              where synchronizer_id = ${synchronizerId.toProtoPrimitive}
                and interval_start >= ${from.toMicros} and interval_start < ${until.toMicros}"""
          .as[(Long, Long)],
        "listTrafficReportIntervals",
      )
      .map(_.map { case (start, trafficConsumed) =>
        CantonTimestamp.assertFromLong(start) -> trafficConsumed
      }.toMap)
}
