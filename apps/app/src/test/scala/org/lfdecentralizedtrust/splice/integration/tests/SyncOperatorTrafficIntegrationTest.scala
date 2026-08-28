// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.Member
import org.lfdecentralizedtrust.splice.codegen.java.splice
import org.lfdecentralizedtrust.splice.codegen.java.splice.round.IssuingMiningRound
import org.lfdecentralizedtrust.splice.codegen.java.splice.types.Round
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.{
  IntegrationTest,
  SpliceTestConsoleEnvironment,
}
import org.lfdecentralizedtrust.splice.util.{DisclosedContracts, SynchronizerFeesTestUtil, WalletTestUtil}

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** The buy path for a registered synchronizer is not exposed through the wallet yet
  * (ChainSafe/canton-extending-mainnet#39), so this exercises `AmuletRules_BuyMemberTraffic`
  * directly with the registration disclosed, which is what the wallet will eventually do.
  */
class SyncOperatorTrafficIntegrationTest
    extends IntegrationTest
    with SynchronizerFeesTestUtil
    with WalletTestUtil {

  private val trafficAmount = 1_000_000L

  override def environmentDefinition: SpliceEnvironmentDefinition =
    EnvironmentDefinition
      .fromResources(
        Seq("simple-topology-1sv.conf", "sync-operator-topology.conf"),
        this.getClass.getSimpleName,
      )
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

      val registration = clue("the DSO registers the synchronizer to this operator") {
        val result = sv1Backend.participantClientWithAdminToken.ledger_api_extensions.commands
          .submitWithResult(
            sv1Backend.config.ledgerApiUser,
            actAs = Seq(dsoParty),
            readAs = Seq(dsoParty),
            update = dsoRules.contractId.exerciseDsoRules_RegisterSynchronizer(
              synchronizerId.toProtoPrimitive,
              operatorParty.toProtoPrimitive,
            ),
          )
        result.exerciseResult.registeredSynchronizerCid
      }

      val limitBefore = extraTrafficLimit(member)

      clue("alice buys traffic for the registered synchronizer") {
        val aliceParty = onboardWalletUser(aliceWalletClient, aliceValidatorBackend)
        aliceWalletClient.tap(walletUsdToAmulet(100.0))
        val transferContext =
          sv1ScanBackend.getTransferContextWithInstances(CantonTimestamp.now())
        val amulets = aliceWalletClient.list().amulets.map(_.contract.contractId.contractId)

        aliceValidatorBackend.participantClientWithAdminToken.ledger_api_extensions.commands
          .submitWithResult(
            aliceValidatorBackend.config.ledgerApiUser,
            actAs = Seq(aliceParty),
            readAs = Seq(aliceParty),
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
                aliceParty.toProtoPrimitive,
                member.toProtoPrimitive,
                synchronizerId.toProtoPrimitive,
                // a registered synchronizer is pinned to migration id 0
                0L,
                trafficAmount,
                Some(dsoParty.toProtoPrimitive).toJava,
                Some(registration).toJava,
              ),
            disclosedContracts = DisclosedContracts
              .forTesting(
                transferContext.amuletRules,
                transferContext.latestOpenMiningRound,
              )
              .toLedgerApiDisclosedContracts,
          )
      }

      clue("the operator grants it on the splitwell sequencer") {
        eventually() {
          extraTrafficLimit(member) shouldBe (limitBefore + trafficAmount)
        }
      }
    }
  }

  private def extraTrafficLimit(
      member: Member
  )(implicit env: SpliceTestConsoleEnvironment): Long =
    syncOperatorBackend.appState.sequencerAdminConnection
      .lookupSequencerTrafficControlState(member)
      .futureValue
      .fold(0L)(_.extraTrafficLimit.value)
}
