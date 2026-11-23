// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.canton.BaseTest
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{Future, Promise}

class SequencerPruningGuardTest extends AsyncWordSpec with BaseTest {

  "SequencerPruningGuard" should {

    "run reads while they are open" in {
      new SequencerPruningGuard().read(Future.successful(1)).map(_ shouldBe 1)
    }

    "fail reads with UNAVAILABLE while they are closed" in {
      val guard = new SequencerPruningGuard()
      for {
        _ <- guard.closeReads()
        failure <- guard.read(Future.successful(1)).failed
      } yield inside(failure) { case e: StatusRuntimeException =>
        e.getStatus.getCode shouldBe Status.Code.UNAVAILABLE
      }
    }

    "close reads once the reads in flight are done" in {
      val guard = new SequencerPruningGuard()
      val inFlight = Promise[Int]()
      val read = guard.read(inFlight.future)
      val closed = guard.closeReads()
      closed.isCompleted shouldBe false
      inFlight.success(1)
      for {
        _ <- read
        _ <- closed
      } yield succeed
    }

    "run reads again once they are opened" in {
      val guard = new SequencerPruningGuard()
      for {
        _ <- guard.closeReads()
        _ = guard.openReads()
        result <- guard.read(Future.successful(1))
      } yield result shouldBe 1
    }
  }
}
