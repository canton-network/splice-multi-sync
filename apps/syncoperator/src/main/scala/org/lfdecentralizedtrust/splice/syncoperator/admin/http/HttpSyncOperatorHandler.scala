// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.admin.http

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import io.opentelemetry.api.trace.Tracer
import org.lfdecentralizedtrust.splice.admin.http.HttpErrorHandler
import org.lfdecentralizedtrust.splice.auth.AuthenticationOnlyAuthExtractor.AuthenticatedRequest
import org.lfdecentralizedtrust.splice.http.v0.{definitions, sync_operator as v0}
import org.lfdecentralizedtrust.splice.syncoperator.TrafficReport
import org.lfdecentralizedtrust.splice.syncoperator.store.TrafficReportStore
import org.lfdecentralizedtrust.splice.util.Codec

import java.time.OffsetDateTime
import scala.concurrent.{ExecutionContext, Future}

class HttpSyncOperatorHandler(
    store: TrafficReportStore,
    synchronizerId: SynchronizerId,
)(implicit
    ec: ExecutionContext,
    tracer: Tracer,
) extends v0.SyncOperatorHandler[AuthenticatedRequest]
    with Spanning {
  private val workflowId = this.getClass.getSimpleName

  override def getTrafficReport(
      respond: v0.SyncOperatorResource.GetTrafficReportResponse.type
  )(
      startRecordTime: OffsetDateTime,
      endRecordTime: OffsetDateTime,
  )(extracted: AuthenticatedRequest): Future[v0.SyncOperatorResource.GetTrafficReportResponse] = {
    implicit val tc: TraceContext = extracted.traceContext
    withSpan(s"$workflowId.getTrafficReport") { _ => _ =>
      val range = for {
        start <- Codec.decode(Codec.OffsetDateTime)(startRecordTime)
        end <- Codec.decode(Codec.OffsetDateTime)(endRecordTime)
        starts <- TrafficReport.intervalStarts(start, end)
      } yield (start, end, starts)
      range match {
        case Left(error) => Future.failed(HttpErrorHandler.badRequest(error))
        case Right((start, end, starts)) =>
          for {
            // Read the latest interval first: intervals are stored in order, so one before it that
            // is missing now stays missing.
            latest <- store.lookupLatestIntervalStart(synchronizerId)
            stored <- store.listIntervals(synchronizerId, start, end)
          } yield respond.OK(
            definitions.GetTrafficReportResponse(TrafficReport.report(starts, stored, latest))
          )
      }
    }
  }
}
