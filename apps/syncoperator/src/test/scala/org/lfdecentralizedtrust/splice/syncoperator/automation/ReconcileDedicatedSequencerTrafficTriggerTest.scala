// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.{
  MediatorId,
  Member,
  ParticipantId,
  SequencerId,
  UniqueIdentifier,
}
import org.lfdecentralizedtrust.splice.syncoperator.automation.ReconcileDedicatedSequencerTrafficTrigger.{
  Task,
  tasks,
}
import org.scalatest.wordspec.AnyWordSpec

class ReconcileDedicatedSequencerTrafficTriggerTest extends AnyWordSpec with BaseTest {

  private val alice: Member = ParticipantId.tryFromProtoPrimitive("PAR::alice::default")
  private val bob: Member = ParticipantId.tryFromProtoPrimitive("PAR::bob::default")
  private val carol: Member = ParticipantId.tryFromProtoPrimitive("PAR::carol::default")
  private val mediator: Member = MediatorId(UniqueIdentifier.tryCreate("mediator", "default"))
  private val sequencer: Member = SequencerId(UniqueIdentifier.tryCreate("sequencer", "default"))

  private val allowance = Some(50L)

  "ReconcileDedicatedSequencerTrafficTrigger.tasks" should {

    "grant each participant its purchased total" in {
      val result = tasks(
        limits = Map(alice -> 0L, bob -> 100L),
        totals = Map(alice -> 100L, bob -> 300L),
        allowance = None,
      )
      result shouldBe Seq(Task(alice, 100L, None), Task(bob, 300L, None))
      result.map(_.target) shouldBe Seq(100L, 300L)
    }

    "leave a participant already at its purchased total alone" in {
      tasks(
        limits = Map(alice -> 100L),
        totals = Map(alice -> 100L),
        allowance = None,
      ) shouldBe empty
    }

    "raise each participant to its purchased total plus the allowance" in {
      val result = tasks(
        limits = Map(alice -> 100L, bob -> 200L),
        totals = Map(alice -> 100L, bob -> 200L),
        allowance,
      )
      result shouldBe Seq(Task(alice, 100L, allowance), Task(bob, 200L, allowance))
      result.map(_.target) shouldBe Seq(150L, 250L)
    }

    "leave a participant already at its purchased total plus the allowance alone" in {
      tasks(
        limits = Map(alice -> 150L),
        totals = Map(alice -> 100L),
        allowance,
      ) shouldBe empty
    }

    "lower a limit a larger allowance raised to the purchased total plus a smaller one" in {
      tasks(
        limits = Map(alice -> 200L),
        totals = Map(alice -> 100L),
        allowance,
      ).map(_.target) shouldBe Seq(150L)
    }

    "lower each limit back to the purchased total once the allowance is removed" in {
      val result = tasks(
        limits = Map(alice -> 150L, bob -> 200L),
        totals = Map(alice -> 100L, bob -> 200L),
        allowance = None,
      )
      result shouldBe Seq(Task(alice, 100L, None))
      result.map(_.target) shouldBe Seq(100L)
    }

    "give a participant with no purchase on record the allowance alone" in {
      val result = tasks(
        limits = Map(carol -> 0L),
        totals = Map.empty,
        allowance,
      )
      result shouldBe Seq(Task(carol, 0L, allowance))
      result.map(_.target) shouldBe Seq(50L)
    }

    "leave a participant with no purchase on record at zero without an allowance" in {
      tasks(
        limits = Map(carol -> 0L),
        totals = Map.empty,
        allowance = None,
      ) shouldBe empty
    }

    "take back the allowance from a participant with no purchase on record once it is removed" in {
      tasks(
        limits = Map(carol -> 50L),
        totals = Map.empty,
        allowance = None,
      ).map(_.target) shouldBe Seq(0L)
    }

    "leave a mediator's unlimited traffic alone, even with a purchase on record" in {
      forEvery(Seq(allowance, None)) { allowance =>
        forEvery(Seq(0L, Long.MaxValue)) { limit =>
          tasks(
            limits = Map(mediator -> limit),
            totals = Map(mediator -> 10L),
            allowance,
          ) shouldBe empty
        }
      }
    }

    "leave a sequencer alone" in {
      tasks(
        limits = Map(sequencer -> 0L),
        totals = Map(sequencer -> 10L),
        allowance,
      ) shouldBe empty
    }

    "wait with a purchase for a member the sequencer has no traffic state for" in {
      tasks(
        limits = Map(alice -> 100L),
        totals = Map(alice -> 100L, carol -> 10L),
        allowance = None,
      ) shouldBe empty
    }

    "list the participants in a fixed order" in {
      tasks(
        limits = Map(carol -> 0L, mediator -> 0L, alice -> 0L, bob -> 0L),
        totals = Map(carol -> 1L, alice -> 1L, bob -> 1L),
        allowance,
      ).map(_.member) shouldBe Seq(alice, bob, carol)
    }
  }

  "ReconcileDedicatedSequencerTrafficTrigger.Task" should {

    "cap a target the allowance would take past the largest limit" in {
      Task(alice, 100L, Some(Long.MaxValue)).target shouldBe Long.MaxValue
      Task(alice, Long.MaxValue, allowance).target shouldBe Long.MaxValue
    }

    "leave a participant at the largest limit alone once its target is capped" in {
      tasks(
        limits = Map(alice -> Long.MaxValue),
        totals = Map(alice -> 100L),
        allowance = Some(Long.MaxValue),
      ) shouldBe empty
    }
  }
}
