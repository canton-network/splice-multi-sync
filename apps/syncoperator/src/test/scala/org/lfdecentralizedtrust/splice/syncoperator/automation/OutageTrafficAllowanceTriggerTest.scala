// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId, UniqueIdentifier}
import org.lfdecentralizedtrust.splice.syncoperator.automation.OutageTrafficAllowanceTrigger.Task
import org.scalatest.wordspec.AnyWordSpec

class OutageTrafficAllowanceTriggerTest extends AnyWordSpec with BaseTest {

  private val alice: Member = ParticipantId.tryFromProtoPrimitive("PAR::alice::default")
  private val bob: Member = ParticipantId.tryFromProtoPrimitive("PAR::bob::default")
  private val carol: Member = ParticipantId.tryFromProtoPrimitive("PAR::carol::default")
  private val mediator: Member = MediatorId(UniqueIdentifier.tryCreate("mediator", "default"))

  private val allowance = Some(50L)

  "OutageTrafficAllowanceTrigger.tasks" should {

    "raise each member to its purchased total plus the allowance" in {
      OutageTrafficAllowanceTrigger.tasks(
        totals = Map(alice -> 100L, bob -> 200L),
        limits = Map(alice -> 100L, bob -> 200L),
        allowance,
      ) shouldBe Seq(Task(alice, 100L, allowance), Task(bob, 200L, allowance))
    }

    "lower a limit a larger allowance raised to the total plus a smaller one" in {
      val tasks = OutageTrafficAllowanceTrigger.tasks(
        totals = Map(alice -> 100L),
        limits = Map(alice -> 200L),
        allowance,
      )
      tasks shouldBe Seq(Task(alice, 100L, allowance))
      tasks.map(_.target) shouldBe Seq(150L)
    }

    "leave a member already at its total plus the allowance alone, so a restart adds nothing" in {
      OutageTrafficAllowanceTrigger.tasks(
        totals = Map(alice -> 100L),
        limits = Map(alice -> 150L),
        allowance,
      ) shouldBe empty
    }

    "set each limit back to the purchased total once the allowance is removed" in {
      val tasks = OutageTrafficAllowanceTrigger.tasks(
        totals = Map(alice -> 100L, bob -> 200L),
        limits = Map(alice -> 150L, bob -> 200L),
        allowance = None,
      )
      tasks shouldBe Seq(Task(alice, 100L, None))
      tasks.map(_.target) shouldBe Seq(100L)
    }

    "raise a limit left below its total by a purchase ingested while the allowance was removed" in {
      OutageTrafficAllowanceTrigger
        .tasks(
          totals = Map(alice -> 300L),
          limits = Map(alice -> 100L),
          allowance = None,
        )
        .map(_.target) shouldBe Seq(300L)
    }

    "skip a member the sequencer has no traffic state for" in {
      OutageTrafficAllowanceTrigger.tasks(
        totals = Map(alice -> 100L, carol -> 10L),
        limits = Map(alice -> 100L),
        allowance,
      ) shouldBe Seq(Task(alice, 100L, allowance))
    }

    "leave a member with no purchase on record alone" in {
      OutageTrafficAllowanceTrigger.tasks(
        totals = Map(alice -> 100L),
        limits = Map(alice -> 100L, carol -> 0L),
        allowance,
      ) shouldBe Seq(Task(alice, 100L, allowance))
    }

    "leave a mediator's unlimited traffic alone, even with a purchase on record" in {
      forEvery(Seq(allowance, None)) { allowance =>
        OutageTrafficAllowanceTrigger.tasks(
          totals = Map(mediator -> 10L),
          limits = Map(mediator -> Long.MaxValue),
          allowance,
        ) shouldBe empty
      }
    }

    "list the members in a fixed order" in {
      OutageTrafficAllowanceTrigger
        .tasks(
          totals = Map(carol -> 1L, alice -> 1L, bob -> 1L),
          limits = Map(carol -> 1L, alice -> 1L, bob -> 1L),
          allowance,
        )
        .map(_.member) shouldBe Seq(alice, bob, carol)
    }
  }

  "OutageTrafficAllowanceTrigger.Task" should {

    "cap a target the allowance would take past the largest limit" in {
      Task(alice, 100L, Some(Long.MaxValue)).target shouldBe Long.MaxValue
    }

    "tell the operator the gap of a limit it could not bring back to the purchased total" in {
      Task(alice, 100L, None).notApplied(150L) should include("50 bytes above")
      Task(alice, 100L, None).notApplied(80L) should include("20 bytes below")
    }

    "tell the operator that the other operators may not have seen the same purchases" in {
      forAll(Seq(Task(alice, 100L, None), Task(alice, 100L, allowance))) { task =>
        task.notApplied(150L) should include("not all seen the same purchases")
      }
    }
  }

  "OutageTrafficAllowanceTrigger.withoutTrafficState" should {

    "list the members with a purchase on record the sequencer has no traffic state for" in {
      OutageTrafficAllowanceTrigger.withoutTrafficState(
        totals = Map(carol -> 100L, alice -> 100L, bob -> 200L, mediator -> 300L),
        limits = Map(alice -> 100L),
      ) shouldBe Seq(bob, carol)
    }
  }

  "OutageTrafficAllowanceTrigger.warningDue" should {

    val interval = PositiveFiniteDuration.ofMinutes(5)
    val warnedAt = CantonTimestamp.Epoch

    "warn about a member not warned about before" in {
      OutageTrafficAllowanceTrigger.warningDue(None, warnedAt, interval) shouldBe true
    }

    "not warn again within the interval" in {
      OutageTrafficAllowanceTrigger.warningDue(
        Some(warnedAt),
        warnedAt.plusSeconds(299),
        interval,
      ) shouldBe false
    }

    "warn again once the interval has passed" in {
      OutageTrafficAllowanceTrigger.warningDue(
        Some(warnedAt),
        warnedAt.plusSeconds(300),
        interval,
      ) shouldBe true
    }
  }
}
