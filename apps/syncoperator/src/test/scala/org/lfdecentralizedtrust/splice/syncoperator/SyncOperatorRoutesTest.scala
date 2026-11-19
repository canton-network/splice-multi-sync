// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.OAuth2BearerToken
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.lfdecentralizedtrust.splice.auth.AuthConfig
import org.lfdecentralizedtrust.splice.http.HttpServerMetrics
import org.lfdecentralizedtrust.splice.http.v0.definitions.TrafficReportInterval
import org.lfdecentralizedtrust.splice.syncoperator.admin.http.HttpSyncOperatorHandler
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.concurrent.Future

class SyncOperatorRoutesTest extends AnyWordSpec with BaseTest with ScalatestRouteTest {

  private val audience = "sync-operator"
  private val secret = "test"

  /** The app's routes, over a reader that answers every request with `read`. */
  private def route(read: Either[String, Seq[TrafficReportInterval]]) = {
    val reader = mock[TrafficReportReader]
    when(reader.read(any[Seq[CantonTimestamp]])(any[TraceContext]))
      .thenReturn(Future.successful(read))
    // Sealed, so rejections become responses like in the server.
    Route.seal(
      SyncOperatorApp.routes(
        new HttpSyncOperatorHandler(TrafficReport(Duration.ofMinutes(10)), reader)(
          executor,
          NoReportingTracerProvider.tracer,
        ),
        AuthConfig.Hs256Unsafe(audience, secret),
        new HttpServerMetrics(new NoOpMetricsFactory, loggerFactory),
        loggerFactory,
      )
    )
  }

  private val request = Get(
    "/api/syncoperator/v0/traffic-report" +
      "?start_record_time=2026-11-19T10:00:00Z&end_record_time=2026-11-19T10:10:00Z"
  )

  private def token(audience: String) =
    OAuth2BearerToken(
      JWT.create().withAudience(audience).withSubject("operator").sign(Algorithm.HMAC256(secret))
    )

  "The sync operator API" should {

    "reject a request without a token" in {
      request ~> route(Right(Seq.empty)) ~> check(status shouldBe StatusCodes.Unauthorized)
    }

    "reject a token for another audience" in {
      request ~> addCredentials(token("wrong-audience")) ~> route(Right(Seq.empty)) ~> check(
        status shouldBe StatusCodes.Unauthorized
      )
    }

    "accept a token for its audience" in {
      request ~> addCredentials(token(audience)) ~> route(Right(Seq.empty)) ~> check(
        status shouldBe StatusCodes.OK
      )
    }

    "answer a range the sequencer can't serve with a 400 and the reason" in {
      request ~> addCredentials(token(audience)) ~> route(Left("the reason")) ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] should include("the reason")
      }
    }
  }
}
