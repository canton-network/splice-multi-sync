// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{Member, ParticipantId}
import org.lfdecentralizedtrust.splice.syncoperator.automation.OutageTrafficAdvanceTrigger.{
  Mode,
  Task,
}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class OutageTrafficAdvanceTriggerTest extends AnyWordSpec with BaseTest {

  private val delay = NonNegativeFiniteDuration.ofMinutes(5)
  private val startedAt = CantonTimestamp.Epoch
  private def minutesAfterStart(minutes: Long) = startedAt.plus(Duration.ofMinutes(minutes))

  private val alice: Member = ParticipantId.tryFromProtoPrimitive("PAR::alice::default")
  private val bob: Member = ParticipantId.tryFromProtoPrimitive("PAR::bob::default")
  private val carol: Member = ParticipantId.tryFromProtoPrimitive("PAR::carol::default")

  "OutageTrafficAdvanceTrigger.mode" should {

    "wait while there is no time yet and the delay has not passed since startup" in {
      OutageTrafficAdvanceTrigger.mode(minutesAfterStart(1), None, startedAt, delay) shouldBe None
    }

    "advance once the delay passes after startup without a time" in {
      OutageTrafficAdvanceTrigger.mode(minutesAfterStart(6), None, startedAt, delay) shouldBe Some(
        Mode.Advanced
      )
    }

    "stay normal while the time is fresh" in {
      OutageTrafficAdvanceTrigger.mode(
        minutesAfterStart(60),
        Some(minutesAfterStart(59)),
        startedAt,
        delay,
      ) shouldBe Some(Mode.Normal)
    }

    "stay normal up to exactly the delay" in {
      OutageTrafficAdvanceTrigger.mode(
        minutesAfterStart(60),
        Some(minutesAfterStart(55)),
        startedAt,
        delay,
      ) shouldBe Some(Mode.Normal)
    }

    "advance once the time is older than the delay" in {
      OutageTrafficAdvanceTrigger.mode(
        minutesAfterStart(60),
        Some(minutesAfterStart(54)),
        startedAt,
        delay,
      ) shouldBe Some(Mode.Advanced)
    }
  }

  "OutageTrafficAdvanceTrigger.tasks" should {

    "raise each member to its total plus the advance" in {
      OutageTrafficAdvanceTrigger.tasks(
        Mode.Advanced,
        totals = Map(alice -> 100L, bob -> 200L),
        limits = Map(alice -> 100L, bob -> 200L),
        advance = 50L,
      ) shouldBe Seq(Task(alice, Mode.Advanced, 150L), Task(bob, Mode.Advanced, 250L))
    }

    "leave a member already at its target alone, so a restarted app does not advance twice" in {
      OutageTrafficAdvanceTrigger.tasks(
        Mode.Advanced,
        totals = Map(alice -> 100L),
        limits = Map(alice -> 150L),
        advance = 50L,
      ) shouldBe empty
    }

    "never lower a limit while advancing" in {
      OutageTrafficAdvanceTrigger.tasks(
        Mode.Advanced,
        totals = Map(alice -> 100L),
        limits = Map(alice -> 900L),
        advance = 50L,
      ) shouldBe empty
    }

    "take the advance back, setting each limit to its total" in {
      OutageTrafficAdvanceTrigger.tasks(
        Mode.Normal,
        totals = Map(alice -> 100L, bob -> 200L),
        limits = Map(alice -> 150L, bob -> 200L),
        advance = 0L,
      ) shouldBe Seq(Task(alice, Mode.Normal, 100L))
    }

    "raise a limit left below its total by a purchase ingested during the take-back" in {
      OutageTrafficAdvanceTrigger.tasks(
        Mode.Normal,
        totals = Map(alice -> 300L),
        limits = Map(alice -> 100L),
        advance = 0L,
      ) shouldBe Seq(Task(alice, Mode.Normal, 300L))
    }

    "skip a member the sequencer has no traffic state for" in {
      OutageTrafficAdvanceTrigger.tasks(
        Mode.Advanced,
        totals = Map(alice -> 100L, carol -> 10L),
        limits = Map(alice -> 100L),
        advance = 50L,
      ) shouldBe Seq(Task(alice, Mode.Advanced, 150L))
    }

    "list the members in a fixed order" in {
      OutageTrafficAdvanceTrigger
        .tasks(
          Mode.Advanced,
          totals = Map(carol -> 1L, alice -> 1L, bob -> 1L),
          limits = Map(carol -> 1L, alice -> 1L, bob -> 1L),
          advance = 1L,
        )
        .map(_.member) shouldBe Seq(alice, bob, carol)
    }
  }
}
