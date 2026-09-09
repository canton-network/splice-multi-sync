// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.validator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeNumeric
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{BaseTest, SynchronizerAlias}
import org.lfdecentralizedtrust.splice.validator.config.BuyExtraTrafficConfig
import org.scalatest.wordspec.AnyWordSpec

class TopupMemberTrafficTriggerTest extends AnyWordSpec with BaseTest {

  // Non-zero on purpose: at migration 0 the registered pin and the DSO's own id are the same
  // literal, which is what every integration test runs at.
  private val domainMigrationId = 7L
  private val pollingInterval = NonNegativeFiniteDuration.ofSeconds(10)
  private val minTopupAmount = 1_000L

  private val globalAlias = SynchronizerAlias.tryCreate("global")
  private val globalId = SynchronizerId.tryFromString("global::domain")
  private val dedicatedAlias = SynchronizerAlias.tryCreate("dedicated")
  private val dedicatedId = SynchronizerId.tryFromString("dedicated::domain")

  private def topup(targetThroughput: Long, minTopupIntervalSeconds: Long) =
    BuyExtraTrafficConfig(
      targetThroughput = NonNegativeNumeric.tryCreate(BigDecimal(targetThroughput)),
      minTopupInterval = NonNegativeFiniteDuration.ofSeconds(minTopupIntervalSeconds),
    )

  // 100 bytes/s over a 60s interval, so 6000 bytes per top-up.
  private val globalTopup = topup(100, 60)
  // 50 bytes/s over a 40s interval, so 2000 bytes: a different amount from the global one.
  private val dedicatedTopup = topup(50, 40)

  private def resolve(
      extra: Seq[(SynchronizerAlias, BuyExtraTrafficConfig)] = Seq.empty,
      connected: Map[SynchronizerAlias, SynchronizerId] = Map.empty,
      required: Set[String] = Set.empty,
  ) =
    TopupMemberTrafficTrigger.resolveTargets(
      topupTargets = (globalAlias, globalTopup) +: extra,
      globalAlias = globalAlias,
      globalSynchronizerId = globalId,
      connectedSynchronizerIds = connected,
      requiredSynchronizerIds = required,
      minTopupAmount = minTopupAmount,
      pollingInterval = pollingInterval,
      domainMigrationId = domainMigrationId,
      logger = logger,
    )

  "TopupMemberTrafficTrigger.resolveTargets" should {

    "take the decentralized synchronizer's id from the ledger and never ask it to register" in {
      val target = resolve(
        // A decoy: the global target's id has to come from the ledger, not from this map.
        connected = Map(globalAlias -> SynchronizerId.tryFromString("decoy::domain"))
      ).loneElement
      target.synchronizerId shouldBe globalId
      target.isGlobal shouldBe true
      // requiredSynchronizerIds is empty here, so only the isGlobal arm keeps it off the
      // registered branch.
      target.needsRegistration shouldBe false
      target.migrationId shouldBe domainMigrationId
    }

    "pin a dedicated synchronizer to migration 0 and price it on its own throughput" in {
      val targets = resolve(
        extra = Seq(dedicatedAlias -> dedicatedTopup),
        connected = Map(dedicatedAlias -> dedicatedId),
      )
      targets.map(_.alias) shouldBe Seq(globalAlias, dedicatedAlias)
      val dedicated = targets.last
      dedicated.synchronizerId shouldBe dedicatedId
      dedicated.needsRegistration shouldBe true
      dedicated.migrationId shouldBe 0L
      dedicated.topupParameters.topupAmount shouldBe 2000L
      targets.head.topupParameters.topupAmount shouldBe 6000L
    }

    "skip an extra synchronizer the participant is not connected to" in {
      // Its traffic state cannot be read, so a target for it would fail every poll.
      resolve(extra = Seq(dedicatedAlias -> dedicatedTopup)).loneElement.alias shouldBe globalAlias
    }

    "buy on membership rather than on a registration for a synchronizer the DSO requires" in {
      val dedicated = resolve(
        extra = Seq(dedicatedAlias -> dedicatedTopup),
        connected = Map(dedicatedAlias -> dedicatedId),
        required = Set(dedicatedId.toProtoPrimitive),
      ).last
      dedicated.needsRegistration shouldBe false
      dedicated.migrationId shouldBe domainMigrationId
    }

    "collapse two aliases that resolve to one synchronizer" in {
      val duplicateOfGlobal = SynchronizerAlias.tryCreate("global-again")
      // resolve prepends the global target, and distinctBy keeps the first.
      val target = resolve(
        extra = Seq(duplicateOfGlobal -> dedicatedTopup),
        connected = Map(duplicateOfGlobal -> globalId),
      ).loneElement
      target.alias shouldBe globalAlias
      target.isGlobal shouldBe true
    }
  }

  "TopupMemberTrafficTrigger.fundedTasks" should {

    // Costs only; what they are attached to does not affect the allocation.
    val candidates = Seq(globalAlias -> BigDecimal(100), dedicatedAlias -> BigDecimal(100))

    "fund only what the balance covers, in order" in {
      // Enough for either one alone, not for both, so the second one misses out.
      val (funded, unfunded) =
        TopupMemberTrafficTrigger.fundedTasks(candidates, Some(BigDecimal(150)))
      funded shouldBe Seq(globalAlias)
      unfunded shouldBe Seq(dedicatedAlias)
    }

    "fund every target when the balance does not bound the purchases" in {
      val (funded, unfunded) = TopupMemberTrafficTrigger.fundedTasks(candidates, None)
      funded shouldBe Seq(globalAlias, dedicatedAlias)
      unfunded shouldBe empty
    }
  }
}
