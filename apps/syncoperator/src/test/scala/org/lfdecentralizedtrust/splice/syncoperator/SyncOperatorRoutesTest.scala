// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.OAuth2BearerToken
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.lfdecentralizedtrust.splice.admin.http.HttpErrorHandler
import org.lfdecentralizedtrust.splice.auth.HMACVerifier
import org.lfdecentralizedtrust.splice.http.HttpServerMetrics
import org.lfdecentralizedtrust.splice.syncoperator.admin.http.HttpSyncOperatorHandler
import org.scalatest.wordspec.AnyWordSpec

class SyncOperatorRoutesTest extends AnyWordSpec with BaseTest with ScalatestRouteTest {

  private val audience = "sync-operator"
  private val secret = "test"

  private val route = {
    implicit val traceContext: TraceContext = TraceContext.empty
    // Sealed, so rejections become responses like in the server. Only the error handler's
    // exceptions directive, since its timeout directive needs the server's request timeout.
    Route.seal(
      new HttpErrorHandler(loggerFactory).exceptionsDirective(traceContext) {
        SyncOperatorApp.routes(
          new HttpSyncOperatorHandler,
          new HMACVerifier(audience, secret),
          new HttpServerMetrics(new NoOpMetricsFactory, loggerFactory),
          loggerFactory,
        )
      }
    )
  }

  private val request = Get("/api/syncoperator/v0/auth-check")

  private def token(audience: String) =
    OAuth2BearerToken(
      JWT.create().withAudience(audience).withSubject("operator").sign(Algorithm.HMAC256(secret))
    )

  "The sync operator API" should {

    "reject a request without a token" in {
      request ~> route ~> check(status shouldBe StatusCodes.Unauthorized)
    }

    "reject a token for another audience" in {
      request ~> addCredentials(token("wrong-audience")) ~> route ~> check(
        status shouldBe StatusCodes.Unauthorized
      )
    }

    "accept a token for its audience" in {
      request ~> addCredentials(token(audience)) ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] should include("operator")
      }
    }
  }
}
