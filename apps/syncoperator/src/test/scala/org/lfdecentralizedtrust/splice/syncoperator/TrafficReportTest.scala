// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  mediatorId,
  participant1,
  participant2,
  participant3,
  participant4,
}
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}

class TrafficReportTest extends AnyWordSpec with BaseTest {

  private val t0 = CantonTimestamp.assertFromInstant(Instant.parse("2026-11-02T10:00:00Z"))

  private val report = TrafficReport(Duration.ofMinutes(10))

  private def interval(n: Long) = t0.plus(Duration.ofMinutes(10L * n))

  private def consumed(bytes: Long) =
    TrafficState.empty.copy(extraTrafficConsumed = NonNegativeLong.tryCreate(bytes))

  "alignDown" should {
    "align to the start of the 10-minute interval" in {
      report.alignDown(interval(1).immediatePredecessor) shouldBe t0
      report.alignDown(interval(1)) shouldBe interval(1)
    }
  }

  "firstReadableStart" should {
    "start strictly after the pruning lower bound" in {
      report.firstReadableStart(t0) shouldBe interval(1)
      report.firstReadableStart(t0.immediateSuccessor) shouldBe interval(1)
    }
  }

  "page" should {
    "list the intervals of an aligned range" in {
      report.page(t0, interval(3)) shouldBe Right(
        TrafficReport.Page(Seq(t0, interval(1), interval(2)), nextStart = None)
      )
    }

    "end a page after 100 intervals and say where the next one starts" in {
      report.page(t0, interval(100)) shouldBe Right(
        TrafficReport.Page((0L until 100L).map(interval), nextStart = None)
      )
      report.page(t0, interval(101)) shouldBe Right(
        TrafficReport.Page((0L until 100L).map(interval), nextStart = Some(interval(100)))
      )
    }

    "reject unaligned and empty ranges" in {
      report.page(t0.immediateSuccessor, interval(1)) shouldBe a[Left[?, ?]]
      report.page(t0, interval(1).immediateSuccessor) shouldBe a[Left[?, ?]]
      report.page(t0, t0) shouldBe a[Left[?, ?]]
      report.page(interval(1), t0) shouldBe a[Left[?, ?]]
    }
  }

  "participantsConsumed" should {
    "sum the participants' consumption between the two readings" in {
      TrafficReport.participantsConsumed(
        atStart = Map(
          participant1 -> consumed(100),
          participant2 -> consumed(50),
        ),
        atEnd = Map(
          participant1 -> consumed(130),
          participant2 -> consumed(50),
          // Joined in between, so it counts from zero.
          participant3 -> consumed(7),
          // Mediators are not participants.
          mediatorId -> consumed(1000),
        ),
      ) shouldBe 37L
    }

    "drop a participant that left in between" in {
      TrafficReport.participantsConsumed(
        atStart = Map(participant1 -> consumed(100), participant4 -> consumed(500)),
        atEnd = Map(participant1 -> consumed(130)),
      ) shouldBe 30L
    }
  }
}
