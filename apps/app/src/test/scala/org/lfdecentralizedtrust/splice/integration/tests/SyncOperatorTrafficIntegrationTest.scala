// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import com.daml.ledger.javaapi.data.CreatedEvent
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{Member, PartyId, SynchronizerId}
import org.lfdecentralizedtrust.splice.codegen.java.splice
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.RegisteredSynchronizer
import org.lfdecentralizedtrust.splice.codegen.java.splice.round.IssuingMiningRound
import org.lfdecentralizedtrust.splice.codegen.java.splice.types.Round
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.{
  IntegrationTest,
  SpliceTestConsoleEnvironment,
}
import org.lfdecentralizedtrust.splice.store.MultiDomainAcsStore.ContractState
import org.lfdecentralizedtrust.splice.util.{
  Contract,
  ContractWithState,
  DisclosedContracts,
  SynchronizerFeesTestUtil,
  WalletTestUtil,
}

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** Buys traffic for a registered synchronizer via `AmuletRules_BuyMemberTraffic` with the
  * registration disclosed, and checks the operator grants it on that synchronizer's sequencer.
  */
class SyncOperatorTrafficIntegrationTest
    extends IntegrationTest
    with SynchronizerFeesTestUtil
    with WalletTestUtil {

  private val firstPurchase = 1_000_000L
  private val secondPurchase = 2_000_000L

  override def environmentDefinition: SpliceEnvironmentDefinition =
    EnvironmentDefinition
      .fromResources(
        Seq("simple-topology-1sv.conf", "sync-operator-topology.conf"),
        this.getClass.getSimpleName,
      )
      .withOnlyAliceValidatorConnectingToSplitwell
      .withStandardSetup

  "sync operator" should {

    "grant traffic purchased for its synchronizer on its own sequencer" in { implicit env =>
      val operatorParty = syncOperatorBackend.appState.store.key.operatorParty
      val dsoParty = sv1Backend.getDsoInfo().dsoParty
      val dsoRules = sv1Backend.getDsoInfo().dsoRules
      // The operator is pointed at the splitwell sequencer, so that is the synchronizer whose
      // traffic it grants. Alice's participant is a member of it.
      val synchronizerId = aliceValidatorBackend.participantClientWithAdminToken.synchronizers
        .id_of(SynchronizerAlias.tryCreate("splitwell"))
        .logical
      val member = aliceValidatorBackend.participantClient.id

      clue("the synchronizer runs traffic control with a zero base rate") {
        synchronizerParameters.trafficControl.map(_.maxBaseTrafficAmount.value) shouldBe Some(0L)
      }

      val registration = clue("the DSO registers the synchronizer to this operator") {
        // The purchase is submitted from alice's participant, which hosts neither the DSO nor
        // the operator, so the registration must be disclosed with its created-event blob.
        val tx = sv1Backend.participantClientWithAdminToken.ledger_api_extensions.commands
          .submitJava(
            actAs = Seq(dsoParty),
            readAs = Seq(dsoParty),
            commands = dsoRules.contractId
              .exerciseDsoRules_RegisterSynchronizer(
                synchronizerId.toProtoPrimitive,
                operatorParty.toProtoPrimitive,
              )
              .commands
              .asScala
              .toSeq,
            userId = sv1Backend.config.ledgerApiUser,
            includeCreatedEventBlob = true,
          )
        val contract = tx.getEventsById.values.asScala
          .collect { case ev: CreatedEvent => ev }
          .flatMap(Contract.fromCreatedEvent(RegisteredSynchronizer.COMPANION)(_))
          .loneElement
        ContractWithState(
          contract,
          ContractState.Assigned(SynchronizerId.tryFromString(tx.getSynchronizerId)),
        )
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
    }

    "grant unlimited traffic to its mediator" in { implicit env =>
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
  }

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
}
