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
import org.lfdecentralizedtrust.splice.http.v0.definitions.TrafficReportInterval.Status
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}

class TrafficReportTest extends AnyWordSpec with BaseTest {

  private val t0 = CantonTimestamp.assertFromInstant(Instant.parse("2026-11-02T10:00:00Z"))

  // Literal 10 minutes rather than TrafficReport.IntervalLength, so the tests pin the length.
  private def interval(n: Long) = t0.plus(Duration.ofMinutes(10L * n))

  private def consumed(bytes: Long) =
    TrafficState.empty.copy(extraTrafficConsumed = NonNegativeLong.tryCreate(bytes))

  "alignDown" should {
    "align to the start of the 10-minute interval" in {
      TrafficReport.alignDown(interval(1).immediatePredecessor) shouldBe t0
      TrafficReport.alignDown(interval(1)) shouldBe interval(1)
    }
  }

  "firstReadableStart" should {
    "start strictly after the pruning lower bound" in {
      TrafficReport.firstReadableStart(t0) shouldBe interval(1)
      TrafficReport.firstReadableStart(t0.immediateSuccessor) shouldBe interval(1)
    }
  }

  "intervalStarts" should {
    "list the intervals of an aligned range" in {
      TrafficReport.intervalStarts(t0, interval(3)) shouldBe Right(
        Seq(t0, interval(1), interval(2))
      )
      TrafficReport.intervalStarts(t0, interval(1000)).map(_.size) shouldBe Right(1000)
    }

    "reject unaligned, empty and too large ranges" in {
      TrafficReport.intervalStarts(t0.immediateSuccessor, interval(1)) shouldBe a[Left[?, ?]]
      TrafficReport.intervalStarts(t0, interval(1).immediateSuccessor) shouldBe a[Left[?, ?]]
      TrafficReport.intervalStarts(t0, t0) shouldBe a[Left[?, ?]]
      TrafficReport.intervalStarts(interval(1), t0) shouldBe a[Left[?, ?]]
      TrafficReport.intervalStarts(t0, interval(1001)) shouldBe a[Left[?, ?]]
    }
  }

  "participantsConsumed" should {
    "sum the participants' consumption between the two readings" in {
      TrafficReport.participantsConsumed(
        atStart = Map(
          participant1 -> consumed(100),
          participant2 -> consumed(50),
          // Left in between, so it is not counted.
          participant4 -> consumed(500),
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
  }

  "report" should {
    "report stored intervals as Ok, earlier gaps as CannotProvide, later ones as Undetermined" in {
      val report = TrafficReport.report(
        starts = (0L until 5L).map(interval),
        stored = Map(interval(1) -> 10L, interval(3) -> 30L),
        latestStored = Some(interval(3)),
      )
      report.map(i => (i.status, i.trafficConsumed)) shouldBe Seq(
        Status.CannotProvide -> None,
        Status.Ok -> Some(10L),
        Status.CannotProvide -> None,
        Status.Ok -> Some(30L),
        Status.Undetermined -> None,
      )
      report.map(_.startRecordTime.toInstant) shouldBe (0L until 5L).map(interval(_).toInstant)
    }

    "report everything as Undetermined before the first interval is stored" in {
      TrafficReport
        .report(Seq(t0), Map.empty, latestStored = None)
        .map(_.status) shouldBe Seq(Status.Undetermined)
    }
  }
}
