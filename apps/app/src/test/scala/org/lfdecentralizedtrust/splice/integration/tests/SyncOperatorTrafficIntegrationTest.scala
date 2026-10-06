// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveLong,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import monocle.macros.syntax.lens.*
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.GovernanceParameters
import org.lfdecentralizedtrust.splice.codegen.java.splice.wallet.topupstate.ValidatorTopUpState
import org.lfdecentralizedtrust.splice.console.ParticipantClientReference
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot
import org.lfdecentralizedtrust.splice.config.ConfigTransforms
import org.lfdecentralizedtrust.splice.http.v0.definitions as d0
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.{
  IntegrationTest,
  SpliceTestConsoleEnvironment,
}
import org.lfdecentralizedtrust.splice.util.{
  SynchronizerFeesTestUtil,
  SyncOperatorTestUtil,
  TriggerTestUtil,
  WalletTestUtil,
}
import org.lfdecentralizedtrust.splice.validator.automation.TopupMemberTrafficTrigger
import org.lfdecentralizedtrust.splice.wallet.store.TxLogEntry
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Buys traffic for a registered synchronizer via `AmuletRules_BuyMemberTraffic` with the
  * registration disclosed, and checks the operator grants it on that synchronizer's sequencer,
  * also through a global-synchronizer outage on the operator's outage traffic allowance.
  */
class SyncOperatorTrafficIntegrationTest
    extends IntegrationTest
    with SynchronizerFeesTestUtil
    with SyncOperatorTestUtil
    with TriggerTestUtil
    with WalletTestUtil {

  private val firstPurchase = 1_000_000L
  private val secondPurchase = 2_000_000L
  private val walletRequestPurchase = 3_000_000L
  private val discount = BigDecimal("0.5")
  private val splitwellAlias = SynchronizerAlias.tryCreate("splitwell")
  private val globalAlias = SynchronizerAlias.tryCreate("global")

  // The outage test's amounts: small enough that a few dozen pings take bob past what was bought
  // for him and on through the allowance.
  private val outageTrafficAllowance = 50_000L
  private val bobPurchase = 50_000L
  // Bought once the global synchronizer is back but before the allowance is removed.
  private val bobPurchaseWithAllowanceSet = 20_000L
  // Less than the shortfall bob is left with once the allowance is removed, so one top-up only
  // covers it if it buys the shortfall too.
  private val bobTopupAmount = 12_000L
  // The same operator app with the allowance set, which the outage test runs in place of
  // syncOperator. Both use one store, so they never run at the same time.
  private val syncOperatorWithAllowanceName = "syncOperatorWithAllowance"

  override def environmentDefinition: SpliceEnvironmentDefinition =
    EnvironmentDefinition
      .fromResources(
        Seq("simple-topology-1sv.conf", "sync-operator-topology.conf"),
        this.getClass.getSimpleName,
      )
      // Only alice and bob connect to splitwell: alice for the purchases, bob for the outage test,
      // which needs a member whose balance starts small.
      .addConfigTransform((_, conf) =>
        conf.copy(
          splitwellApps = Map.empty,
          validatorApps =
            conf.validatorApps.updatedWith(InstanceName.tryCreate("splitwellValidator")) {
              _.map(c => c.copy(domains = c.domains.copy(extra = Seq.empty)))
            },
        )
      )
      .withStandardSetup
      // Down from 200 KB, so bob's small purchases are allowed.
      .addConfigTransform((_, conf) =>
        ConfigTransforms.updateAllSvAppFoundDsoConfigs_(
          _.focus(_.initialSynchronizerFeesConfig.minTopupAmount)
            .replace(NonNegativeLong.tryCreate(10_000L))
        )(conf)
      )
      // withStandardSetup turns top-ups on for the decentralized synchronizer. Move alice's and
      // bob's targets onto splitwell instead, so the operator's synchronizer is the only one they
      // top up automatically.
      .addConfigTransform((_, config) =>
        ConfigTransforms.updateAllValidatorConfigs { case (name, validatorConfig) =>
          if (name == "bobValidator")
            validatorConfig
              .focus(_.domains.global.buyExtraTraffic.targetThroughput)
              .replace(NonNegativeNumeric.tryCreate(BigDecimal(0)))
              .focus(_.domains.extra)
              .modify(_.map { extra =>
                if (extra.alias == splitwellAlias)
                  extra
                    // 100 bytes a second over a two-minute interval, so bobTopupAmount per top-up
                    .focus(_.topup.targetThroughput)
                    .replace(NonNegativeNumeric.tryCreate(BigDecimal(100)))
                    .focus(_.topup.minTopupInterval)
                    .replace(NonNegativeFiniteDuration.ofMinutes(2))
                else extra
              })
              // Resumed only once the outage test needs bob's top-up to cover his shortfall.
              .focus(_.automation)
              .modify(_.withPausedTrigger[TopupMemberTrafficTrigger])
          else if (name == "aliceValidator")
            validatorConfig
              .focus(_.domains.global.buyExtraTraffic.targetThroughput)
              .replace(NonNegativeNumeric.tryCreate(BigDecimal(0)))
              .focus(_.domains.extra)
              .modify(_.map { extra =>
                if (extra.alias == splitwellAlias)
                  extra
                    .focus(_.topup.targetThroughput)
                    .replace(NonNegativeNumeric.tryCreate(BigDecimal(100000)))
                    .focus(_.topup.minTopupInterval)
                    .replace(NonNegativeFiniteDuration.ofMinutes(1))
                else extra
              })
              // Resumed only at the end of the first test: a trigger buying on splitwell for the
              // same member would break the exact-equality assertions on the two manual purchases.
              // Alice's and bob's validators only: pausing a trigger an app does not register warns,
              // and the SV validator never registers this one.
              .focus(_.automation)
              .modify(_.withPausedTrigger[TopupMemberTrafficTrigger])
          else validatorConfig
        }(config)
      )
      // Last, so it is a copy of syncOperator as every transform above left it.
      .addConfigTransform { (_, conf) =>
        val allowance = Some(PositiveLong.tryCreate(outageTrafficAllowance))
        val syncOperator = conf.syncOperatorApps(InstanceName.tryCreate("syncOperator"))
        conf.copy(syncOperatorApps =
          conf.syncOperatorApps.updated(
            InstanceName.tryCreate(syncOperatorWithAllowanceName),
            syncOperator.copy(outageTrafficAllowance = allowance),
          )
        )
      }
      // Every app but syncOperatorWithAllowance starts as it would without manual start, and the
      // multi-synchronizer flag that withManualStart's reset drops is set again once they have.
      .withManualStart
      .withAdditionalSetup(implicit env =>
        startAllSync(
          env.amuletNodes.local
            .distinctBy(_.name)
            .filterNot(_.name == syncOperatorWithAllowanceName)*
        )
      )
      .withMultiSyncFeatureFlag()

  "sync operator" should {

    "grant traffic purchased for its synchronizer on its own sequencer" in { implicit env =>
      val operatorParty = syncOperatorBackend.appState.store.key.operatorParty
      val dsoParty = sv1Backend.getDsoInfo().dsoParty
      val dsoRules = sv1Backend.getDsoInfo().dsoRules
      // The operator is pointed at the splitwell sequencer, so that is the synchronizer whose
      // traffic it grants. Alice's participant is a member of it.
      val synchronizerId = aliceValidatorBackend.participantClientWithAdminToken.synchronizers
        .id_of(splitwellAlias)
        .logical
      val member = aliceValidatorBackend.participantClient.id

      clue("the synchronizer runs traffic control with a zero base rate") {
        synchronizerParameters.trafficControl.map(_.maxBaseTrafficAmount.value) shouldBe Some(0L)
      }

      clue("the synchronizer charges traffic the way the global synchronizer does") {
        val participant = aliceValidatorBackend.participantClientWithAdminToken
        val globalSynchronizerId =
          participant.synchronizers.id_of(SynchronizerAlias.tryCreate("global")).logical
        eventually() {
          val global = participant.topology.synchronizer_parameters
            .get_dynamic_synchronizer_parameters(globalSynchronizerId)
            .trafficControl
            .value
          val dedicated = synchronizerParameters.trafficControl.value
          dedicated.readVsWriteScalingFactor shouldBe global.readVsWriteScalingFactor
          dedicated.freeConfirmationResponses shouldBe global.freeConfirmationResponses
        }
      }

      clue("the mediator is granted unlimited traffic") {
        val mediator = syncOperatorBackend.appState.sequencerAdminConnection
          .getMediatorSynchronizerState(
            syncOperatorBackend.appState.store.key.synchronizerId,
            TopologySnapshot.Effective,
          )
          .futureValue
          .mapping
          .active
          .forgetNE
          .loneElement
        eventually() {
          extraTrafficLimit(mediator) shouldBe NonNegativeLong.maxValue.value
        }
      }

      clue("the DSO registers the synchronizer to this operator") {
        sv1Backend.participantClientWithAdminToken.ledger_api_extensions.commands
          .submitJava(
            actAs = Seq(dsoParty),
            readAs = Seq(dsoParty),
            commands = dsoRules.contractId
              .exerciseDsoRules_RegisterSynchronizer(
                synchronizerId.toProtoPrimitive,
                operatorParty.toProtoPrimitive,
                new GovernanceParameters(discount.bigDecimal.setScale(10)),
              )
              .commands
              .asScala
              .toSeq,
            userId = sv1Backend.config.ledgerApiUser,
          )
      }

      // Alice's participant hosts neither the DSO nor the operator, so the registration has to
      // be disclosed, and Scan is the only source of its created-event blob.
      sv1ScanBackend.lookupSynchronizerRegistration("dedicated::does-not-exist") shouldBe None
      val registration = eventually() {
        sv1ScanBackend.lookupSynchronizerRegistration(synchronizerId.toProtoPrimitive).value
      }

      clue("the registration carries the discount") {
        BigDecimal(registration.payload.governanceParameters.discountFactor) shouldBe discount
      }

      clue("the validator serves the registration to its wallet clients through the scan proxy") {
        aliceValidatorBackend.scanProxy
          .lookupSynchronizerRegistration(synchronizerId.toProtoPrimitive)
          .value
          .contractId shouldBe registration.contractId
      }

      val aliceParty = onboardWalletUser(aliceWalletClient, aliceValidatorBackend)
      aliceWalletClient.tap(walletUsdToAmulet(200.0))

      clue("no traffic is granted before any purchase") {
        extraTrafficLimit(member) shouldBe 0L
        // With a zero base rate the member has no allowance at all, so it cannot transact
        // until a purchase is granted.
        trafficState(member).map(_.state.baseTrafficRemainder.value) shouldBe Some(0L)
      }

      val (dedicatedPurchase, _) = actAndCheck(
        "alice buys traffic for the splitwell synchronizer",
        buyTraffic(aliceParty, member, synchronizerId, Some(registration), dsoParty, firstPurchase),
      )(
        "the purchase is granted on the splitwell sequencer",
        _ => extraTrafficLimit(member) shouldBe firstPurchase,
      )

      // Alice's topology broadcasts were bounced by the zero base rate; once granted they go
      // through and consume the purchased traffic.
      clue("the granted traffic is drawn down") {
        eventually() {
          trafficState(member).map(_.extraTrafficConsumed.value).getOrElse(0L) should be > 0L
        }
      }

      clue("the purchase costs the global synchronizer's price at the discount") {
        val globalSynchronizerId =
          aliceValidatorBackend.participantClientWithAdminToken.synchronizers
            .id_of(SynchronizerAlias.tryCreate("global"))
            .logical
        val globalPurchase =
          buyTraffic(aliceParty, member, globalSynchronizerId, None, dsoParty, firstPurchase)
        // Allows for Daml rounding each step to ten decimal places.
        BigDecimal(dedicatedPurchase.amuletPaid) shouldBe
          (BigDecimal(globalPurchase.amuletPaid) * discount +- BigDecimal("0.000001"))
      }

      clue("a purchase for a synchronizer that is neither required nor registered is refused") {
        val unregisteredSynchronizerId =
          SynchronizerId.tryFromString(
            s"unregistered::${synchronizerId.namespace.toProtoPrimitive}"
          )
        assertThrowsAndLogsCommandFailures(
          buyTraffic(aliceParty, member, unregisteredSynchronizerId, None, dsoParty, firstPurchase),
          _.errorMessage should include("Unknown synchronizer provided"),
        )
      }

      actAndCheck(
        "alice buys a second traffic amount",
        buyTraffic(aliceParty, member, synchronizerId, Some(registration), dsoParty, secondPurchase),
      )(
        "the limit rises by exactly the second amount",
        _ => extraTrafficLimit(member) shouldBe (firstPurchase + secondPurchase),
      )

      // The end-user path: the wallet resolves and discloses the registration itself.
      val manualAndWalletPurchases = firstPurchase + secondPurchase + walletRequestPurchase
      actAndCheck(
        "alice requests traffic for splitwell through her wallet",
        aliceWalletClient.createBuyTrafficRequest(
          aliceValidatorBackend.getValidatorPartyId(),
          synchronizerId,
          walletRequestPurchase,
          "splitwell-request",
          env.environment.clock.now.plus(java.time.Duration.ofMinutes(1)),
        ),
      )(
        "the request completes and is granted on the splitwell sequencer",
        _ => {
          inside(aliceWalletClient.getTrafficRequestStatus("splitwell-request")) {
            case d0.GetBuyTrafficRequestStatusResponse.members.BuyTrafficRequestCompletedResponse(
                  d0.BuyTrafficRequestCompletedResponse(status, _)
                ) =>
              status shouldBe TxLogEntry.Http.BuyTrafficRequestStatus.Completed
          }
          extraTrafficLimit(member) shouldBe manualAndWalletPurchases
        },
      )

      // The validator's own top-up trigger does the same buy unattended, on splitwell only:
      // alice's global target is zero, so the fan-out is the only reason it runs at all.
      val topupTrigger = aliceValidatorBackend.appState.automation
        .trigger[TopupMemberTrafficTrigger]
      setTriggersWithin(triggersToResumeAtStart = Seq(topupTrigger)) {
        clue("the trigger tops up splitwell on its own") {
          eventually(2.minutes) {
            extraTrafficLimit(member) should be > manualAndWalletPurchases
          }
        }
        clue("it holds a top-up state for splitwell, which only the fan-out creates") {
          inside(
            listValidatorContracts(ValidatorTopUpState.COMPANION)(
              aliceValidatorBackend,
              _.data.synchronizerId == synchronizerId.toProtoPrimitive,
            )
          ) { case Seq(topupState) =>
            topupState.data.memberId shouldBe member.toProtoPrimitive
          }
        }
      }
    }

    "keep members transacting through a global-synchronizer outage on the operator's allowance" in {
      implicit env =>
        val synchronizerId = syncOperatorBackend.appState.store.key.synchronizerId
        val dsoParty = sv1Backend.getDsoInfo().dsoParty
        // Registered by the test above.
        val registration = eventually() {
          sv1ScanBackend.lookupSynchronizerRegistration(synchronizerId.toProtoPrimitive).value
        }
        val aliceParticipant = aliceValidatorBackend.participantClientWithAdminToken
        val alice = aliceParticipant.id
        val bobParticipant = bobValidatorBackend.participantClientWithAdminToken
        val bob = bobParticipant.id
        // The operator app sees purchases, which are made on the global synchronizer, through
        // this participant.
        val operatorParticipant = splitwellValidatorBackend.participantClientWithAdminToken
        val syncOperatorWithAllowance = syncop(syncOperatorWithAllowanceName)
        def purchased() =
          runningSyncOperatorBackend.appState.store.listTotalPurchasedMemberTraffic().futureValue
        def limitsAreTotalsPlus(allowance: Long, totals: Map[Member, Long]) =
          forAll(totals) { case (member, total) =>
            extraTrafficLimit(member) shouldBe total + allowance
          }

        clue("bob holds no traffic on splitwell yet") {
          extraTrafficLimit(bob) shouldBe 0L
        }

        // Anyone can fund a member's traffic, so alice pays for bob's.
        val aliceParty = onboardWalletUser(aliceWalletClient, aliceValidatorBackend)
        aliceWalletClient.tap(walletUsdToAmulet(50.0))
        actAndCheck(
          "alice buys a little traffic for bob on splitwell",
          buyTraffic(aliceParty, bob, synchronizerId, Some(registration), dsoParty, bobPurchase),
        )(
          "the purchase is granted",
          _ => extraTrafficLimit(bob) shouldBe bobPurchase,
        )
        val totals = purchased()
        totals.keySet shouldBe Set(alice, bob)

        try {
          // The other apps on the operator's participant warn about the submissions that fail
          // while it is cut off from the global synchronizer, so this tolerates their warnings.
          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            {
              // Cut off from the global synchronizer, the operator's participant sees no
              // purchases, which is what an outage looks like to the operator.
              operatorParticipant.synchronizers.disconnect(globalAlias)

              actAndCheck(
                "the operator sets the allowance and restarts its app", {
                  syncOperatorBackend.stop()
                  syncOperatorWithAllowance.startSync()
                },
              )(
                "each member's limit rises to its purchased total plus the allowance, and the app " +
                  "warns that the allowance is set",
                _ => {
                  limitsAreTotalsPlus(outageTrafficAllowance, totals)
                  recordedWarnings(allowanceSetWarning) should not be empty
                },
              )

              clue("a restart with the allowance still set moves no limit") {
                syncOperatorWithAllowance.stop()
                syncOperatorWithAllowance.startSync()
                always(durationOfSuccess = 10.seconds, pollIntervalMs = 1000) {
                  limitsAreTotalsPlus(outageTrafficAllowance, totals)
                }
              }

              clue("bob keeps transacting past what was bought for him") {
                pingWhile(bobParticipant, synchronizerId)(consumed(bob) <= bobPurchase)
                consumed(bob) should be > bobPurchase
              }

              clue("and is refused once he has used the allowance") {
                Iterator
                  .continually(Try(ping(bobParticipant, synchronizerId)))
                  .take(maxPings)
                  .exists(_.isFailure) shouldBe true
                consumed(bob) should be <= bobPurchase + outageTrafficAllowance
              }

              clue("alice runs on what was bought for her, without the allowance") {
                consumed(alice) should be <= totals(alice)
              }
            },
            // Once per start of the app.
            logs => forAtLeast(2, logs)(_.warningMessage should include(allowanceSetWarning)),
          )

          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            {
              operatorParticipant.synchronizers.reconnect(globalAlias)

              actAndCheck(
                "alice buys more traffic for bob once the global synchronizer is back",
                buyTraffic(
                  aliceParty,
                  bob,
                  synchronizerId,
                  Some(registration),
                  dsoParty,
                  bobPurchaseWithAllowanceSet,
                ),
              )(
                "the operator warns that the allowance is still set",
                _ => recordedWarnings(purchaseLandedWarning) should not be empty,
              )

              clue("the purchase pays down bob's credit instead of raising his limit") {
                purchased()(bob) shouldBe bobPurchase + bobPurchaseWithAllowanceSet
                extraTrafficLimit(bob) shouldBe bobPurchase + outageTrafficAllowance
              }

              // In here, as the app warns that the allowance is set until it has stopped.
              syncOperatorWithAllowance.stop()
            },
            logs => forAtLeast(1, logs)(_.warningMessage should include(purchaseLandedWarning)),
          )
        } finally {
          if (
            !operatorParticipant.synchronizers
              .list_connected()
              .exists(_.synchronizerAlias == globalAlias)
          )
            operatorParticipant.synchronizers.reconnect(globalAlias)
        }

        val totalsWithoutAllowance = actAndCheck(
          "the operator restarts its app without the allowance", {
            syncOperatorBackend.startSync()
            purchased()
          },
        )(
          "each member's limit goes back down to its purchased total",
          totals => limitsAreTotalsPlus(0L, totals),
        )._1

        clue("alice, who did not use the allowance, keeps transacting") {
          ping(aliceParticipant, synchronizerId)
        }

        clue("bob used more than was bought for him, so he is refused until a purchase covers it") {
          consumed(bob) should be > totalsWithoutAllowance(bob)
          assertThrowsAndLogsCommandFailures(
            ping(bobParticipant, synchronizerId),
            _.errorMessage should include("Unable to ping"),
          )
        }

        val topupTrigger =
          bobValidatorBackend.appState.automation.trigger[TopupMemberTrafficTrigger]
        setTriggersWithin(triggersToResumeAtStart = Seq(topupTrigger)) {
          clue("one top-up buys bob's shortfall with it, and he transacts again") {
            // Inside bob's two-minute top-up interval, so a single purchase covered it.
            eventually(100.seconds) {
              extraTrafficLimit(bob) should be > consumed(bob)
            }
            // More than the top-up alone, which is less than the shortfall.
            purchased()(bob) - totalsWithoutAllowance(bob) should be > bobTopupAmount
            ping(bobParticipant, synchronizerId)
          }
        }
    }
  }

  private val allowanceSetWarning =
    s"The outage traffic allowance of $outageTrafficAllowance bytes is set, covering 2 members"
  private val purchaseLandedWarning =
    "was bought or merged on the global synchronizer while the outage traffic allowance is set"

  // The warnings suppressed so far in the enclosing log assertion, for waiting on one.
  private def recordedWarnings(text: String) =
    loggerFactory.fetchRecordedLogEntries.filter(_.message.contains(text))

  // Bounds the ping loops, so a ping that costs less than expected fails the test instead of
  // running forever.
  private val maxPings = 300

  private def ping(participant: ParticipantClientReference, synchronizerId: SynchronizerId) =
    participant.health.ping(participant.id, synchronizerId = Some(synchronizerId))

  private def pingWhile(participant: ParticipantClientReference, synchronizerId: SynchronizerId)(
      condition: => Boolean
  ): Unit =
    Iterator
      .continually(())
      .take(maxPings)
      .takeWhile(_ => condition)
      .foreach(_ => ping(participant, synchronizerId))

  private def consumed(member: Member)(implicit env: SpliceTestConsoleEnvironment): Long =
    trafficState(member).fold(0L)(_.extraTrafficConsumed.value)

  // The sync-operator CI job bootstraps splitwell with traffic control and a zero base rate;
  // see start-canton.sh -t.
  private def synchronizerParameters(implicit env: SpliceTestConsoleEnvironment) =
    syncOperatorBackend.appState.sequencerAdminConnection
      .getSynchronizerParametersState(syncOperatorBackend.appState.store.key.synchronizerId)
      .futureValue
      .mapping
      .parameters

}
