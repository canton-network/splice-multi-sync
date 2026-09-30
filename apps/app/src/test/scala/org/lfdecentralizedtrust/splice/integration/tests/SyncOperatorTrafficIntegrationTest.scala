// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, NonNegativeNumeric}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{Member, PartyId, SynchronizerId}
import monocle.macros.syntax.lens.*
import org.lfdecentralizedtrust.splice.codegen.java.splice
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.GovernanceParameters
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.RegisteredSynchronizer
import org.lfdecentralizedtrust.splice.codegen.java.splice.round.IssuingMiningRound
import org.lfdecentralizedtrust.splice.codegen.java.splice.wallet.topupstate.ValidatorTopUpState
import org.lfdecentralizedtrust.splice.codegen.java.splice.types.Round
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot
import org.lfdecentralizedtrust.splice.config.ConfigTransforms
import org.lfdecentralizedtrust.splice.console.ParticipantClientReference
import org.lfdecentralizedtrust.splice.http.v0.definitions as d0
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.{
  IntegrationTest,
  SpliceTestConsoleEnvironment,
}
import org.lfdecentralizedtrust.splice.util.{
  ContractWithState,
  DisclosedContracts,
  SynchronizerFeesTestUtil,
  TriggerTestUtil,
  WalletTestUtil,
}
import org.lfdecentralizedtrust.splice.validator.automation.TopupMemberTrafficTrigger
import org.lfdecentralizedtrust.splice.wallet.store.TxLogEntry

import scala.concurrent.duration.*
import scala.util.Try
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** Buys traffic for a registered synchronizer via `AmuletRules_BuyMemberTraffic` with the
  * registration disclosed, and checks the operator grants it on that synchronizer's sequencer.
  */
class SyncOperatorTrafficIntegrationTest
    extends IntegrationTest
    with SynchronizerFeesTestUtil
    with TriggerTestUtil
    with WalletTestUtil {

  private val firstPurchase = 1_000_000L
  private val secondPurchase = 2_000_000L
  private val walletRequestPurchase = 3_000_000L
  private val splitwellAlias = SynchronizerAlias.tryCreate("splitwell")

  // The outage traffic advance on the registration, and bob's purchase in the outage test: small
  // enough that a few dozen pings take bob past what he bought and on to the cap.
  private val outageAdvance = 50_000L
  private val bobPurchase = 50_000L
  private val outageAdvanceDelay = NonNegativeFiniteDuration.ofSeconds(10)

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
          validatorApps = conf.validatorApps.updatedWith(InstanceName.tryCreate("splitwellValidator")) {
            _.map(c => c.copy(domains = c.domains.copy(extra = Seq.empty)))
          },
        )
      )
      .withStandardSetup
      // Down from 200 KB, so bob's small purchase is allowed.
      .addConfigTransform((_, conf) =>
        ConfigTransforms.updateAllSvAppFoundDsoConfigs_(
          _.focus(_.initialSynchronizerFeesConfig.minTopupAmount)
            .replace(NonNegativeLong.tryCreate(10_000L))
        )(conf)
      )
      .addConfigTransform((_, conf) =>
        ConfigTransforms.updateAllSyncOperatorAppConfigs_(
          _.copy(outageAdvanceDelay = outageAdvanceDelay)
        )(conf)
      )
      // withStandardSetup turns top-ups on for the decentralized synchronizer. Move alice's
      // target onto splitwell instead, so the operator's synchronizer is the only one she tops
      // up automatically.
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
                    // 60 KB per top-up
                    .focus(_.topup.targetThroughput)
                    .replace(NonNegativeNumeric.tryCreate(BigDecimal(1000)))
                    .focus(_.topup.minTopupInterval)
                    .replace(NonNegativeFiniteDuration.ofMinutes(1))
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
              // Resumed only at the end of the test: a trigger buying on splitwell for the same
              // member would break the exact-equality assertions on the two manual purchases.
              // Alice's validator only: pausing a trigger an app does not register warns, and the
              // SV validator never registers this one.
              .focus(_.automation)
              .modify(_.withPausedTrigger[TopupMemberTrafficTrigger])
          else validatorConfig
        }(config)
      )

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
                new GovernanceParameters(java.math.BigDecimal.ONE.setScale(10), outageAdvance),
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

      actAndCheck(
        "alice buys traffic for the splitwell synchronizer",
        buyTraffic(aliceParty, member, synchronizerId, registration, dsoParty, firstPurchase),
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

      actAndCheck(
        "alice buys a second traffic amount",
        buyTraffic(aliceParty, member, synchronizerId, registration, dsoParty, secondPurchase),
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

    "advance traffic through a global-synchronizer outage and take it back" in { implicit env =>
      val synchronizerId = syncOperatorBackend.appState.store.key.synchronizerId
      val dsoParty = sv1Backend.getDsoInfo().dsoParty
      val registration = eventually() {
        sv1ScanBackend.lookupSynchronizerRegistration(synchronizerId.toProtoPrimitive).value
      }
      registration.payload.governanceParameters.outageAdvance shouldBe outageAdvance
      val bobParticipant = bobValidatorBackend.participantClientWithAdminToken
      val bob = bobParticipant.id
      // The operator app reads the global synchronizer's time through this participant.
      val operatorParticipant = splitwellValidatorBackend.participantClientWithAdminToken
      val globalAlias = syncOperatorBackend.config.globalSynchronizerAlias

      clue("bob holds no traffic on splitwell yet") {
        extraTrafficLimit(bob) shouldBe 0L
      }

      // Anyone can fund a member's traffic, so alice pays for bob's.
      val aliceParty = onboardWalletUser(aliceWalletClient, aliceValidatorBackend)
      aliceWalletClient.tap(walletUsdToAmulet(50.0))
      actAndCheck(
        "alice buys a little traffic for bob on splitwell",
        buyTraffic(aliceParty, bob, synchronizerId, registration, dsoParty, bobPurchase),
      )(
        "the purchase is granted",
        _ => extraTrafficLimit(bob) shouldBe bobPurchase,
      )

      // Cut off from the global synchronizer, the operator's participant sees its time stop,
      // which is what an outage looks like to the operator. The other apps on that participant
      // warn about the submissions that fail until it reconnects.
      loggerFactory.suppressWarningsAndErrors {
        operatorParticipant.synchronizers.disconnect(globalAlias)
        try {
          clue("the advance applies once the global synchronizer has been unreachable for the delay") {
            eventually(1.minute) {
              extraTrafficLimit(bob) shouldBe bobPurchase + outageAdvance
            }
          }
          clue("a restart during the outage keeps the advance and does not add it again") {
            syncOperatorBackend.stop()
            syncOperatorBackend.startSync()
            // Longer than the delay, which the restarted app waits out before it acts again.
            always(durationOfSuccess = 20.seconds, pollIntervalMs = 1000) {
              extraTrafficLimit(bob) shouldBe bobPurchase + outageAdvance
            }
          }
          clue("bob keeps transacting past what was bought for him") {
            pingWhile(bobParticipant, synchronizerId)(consumed(bob) <= bobPurchase)
            consumed(bob) should be > bobPurchase
          }
          clue("and is rejected at the cap") {
            Iterator
              .continually(Try(ping(bobParticipant, synchronizerId)))
              .take(maxPings)
              .exists(_.isFailure) shouldBe true
            consumed(bob) should be <= (bobPurchase + outageAdvance)
          }
        } finally operatorParticipant.synchronizers.reconnect(globalAlias)
      }

      clue("the advance is taken back once the global synchronizer is reachable again") {
        eventually(1.minute) {
          extraTrafficLimit(bob) shouldBe bobPurchase
        }
      }

      clue("bob is below zero, so he cannot transact until a purchase covers it") {
        consumed(bob) should be > bobPurchase
        assertThrowsAndLogsCommandFailures(
          ping(bobParticipant, synchronizerId),
          _.errorMessage should include("ping"),
        )
      }

      val topupTrigger = bobValidatorBackend.appState.automation.trigger[TopupMemberTrafficTrigger]
      setTriggersWithin(triggersToResumeAtStart = Seq(topupTrigger)) {
        clue("one top-up covers the shortfall, and bob transacts again") {
          // Inside the one-minute top-up interval, so a single purchase covered it.
          eventually(50.seconds) {
            extraTrafficLimit(bob) should be > consumed(bob)
          }
          ping(bobParticipant, synchronizerId)
        }
      }
    }
  }

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

  private def buyTraffic(
      buyer: PartyId,
      member: Member,
      synchronizerId: SynchronizerId,
      registration: ContractWithState[RegisteredSynchronizer.ContractId, RegisteredSynchronizer],
      dsoParty: PartyId,
      trafficAmount: Long,
  )(implicit env: SpliceTestConsoleEnvironment): Unit = {
    val transferContext =
      sv1ScanBackend.getTransferContextWithInstances(CantonTimestamp.now())
    val amulets = aliceWalletClient.list().amulets.map(_.contract.contractId.contractId)

    aliceValidatorBackend.participantClientWithAdminToken.ledger_api_extensions.commands
      .submitWithResult(
        aliceValidatorBackend.config.ledgerApiUser,
        actAs = Seq(buyer),
        readAs = Seq(buyer),
        update = transferContext.amuletRules.contract.contractId
          .exerciseAmuletRules_BuyMemberTraffic(
            amulets
              .map[splice.amuletrules.TransferInput](cid =>
                new splice.amuletrules.transferinput.InputAmulet(
                  new splice.amulet.Amulet.ContractId(cid)
                )
              )
              .asJava,
            new splice.amuletrules.TransferContext(
              transferContext.latestOpenMiningRound.contract.contractId,
              Map.empty[Round, IssuingMiningRound.ContractId].asJava,
              Map.empty[String, splice.amulet.ValidatorRight.ContractId].asJava,
              None.toJava,
            ),
            buyer.toProtoPrimitive,
            member.toProtoPrimitive,
            synchronizerId.toProtoPrimitive,
            // a registered synchronizer is pinned to migration id 0
            0L,
            trafficAmount,
            Some(dsoParty.toProtoPrimitive).toJava,
            Some(registration.contractId).toJava,
          ),
        disclosedContracts = DisclosedContracts
          .forTesting(
            transferContext.amuletRules,
            transferContext.latestOpenMiningRound,
            registration,
          )
          .toLedgerApiDisclosedContracts,
      )
  }

  // The sync-operator CI job bootstraps splitwell with traffic control and a zero base rate;
  // see start-canton.sh -t.
  private def synchronizerParameters(implicit env: SpliceTestConsoleEnvironment) =
    syncOperatorBackend.appState.sequencerAdminConnection
      .getSynchronizerParametersState(syncOperatorBackend.appState.store.key.synchronizerId)
      .futureValue
      .mapping
      .parameters

  private def trafficState(
      member: Member
  )(implicit env: SpliceTestConsoleEnvironment): Option[SequencerAdminConnection.TrafficState] =
    syncOperatorBackend.appState.sequencerAdminConnection
      .lookupSequencerTrafficControlState(member)
      .futureValue

  private def extraTrafficLimit(
      member: Member
  )(implicit env: SpliceTestConsoleEnvironment): Long =
    trafficState(member).fold(0L)(_.extraTrafficLimit.value)

  private def consumed(
      member: Member
  )(implicit env: SpliceTestConsoleEnvironment): Long =
    trafficState(member).fold(0L)(_.extraTrafficConsumed.value)
}
