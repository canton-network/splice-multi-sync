package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import monocle.Monocle.toAppliedFocusOps
import org.lfdecentralizedtrust.splice.auth.AuthUtil
import org.lfdecentralizedtrust.splice.codegen.java.da.time.types.RelTime
import org.lfdecentralizedtrust.splice.codegen.java.splice
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.RegisteredSynchronizer
import org.lfdecentralizedtrust.splice.codegen.java.splice.dsorules.DsoRules_RegisterSynchronizer
import org.lfdecentralizedtrust.splice.codegen.java.splice.dsorules.actionrequiringconfirmation.ARC_DsoRules
import org.lfdecentralizedtrust.splice.codegen.java.splice.dsorules.dsorules_actionrequiringconfirmation.SRARC_RegisterSynchronizer
import org.lfdecentralizedtrust.splice.codegen.java.splice.round.IssuingMiningRound
import org.lfdecentralizedtrust.splice.codegen.java.splice.types.Round
import org.lfdecentralizedtrust.splice.console.ParticipantClientReference
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.IntegrationTestWithIsolatedEnvironment
import org.lfdecentralizedtrust.splice.util.{ContractWithState, DisclosedContracts}

import java.time.Duration
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*
import scala.sys.process.*

/** Verifies that the sync operator serves the app-synchronizer as a dedicated synchronizer: it
  * holds off until the DSO registers the synchronizer, then narrows the base rate to zero, and a
  * member transacts on it only against traffic it has bought.
  *
  * This spins up the docker-compose localnet with the sync operator enabled (-O)
  */
class LocalNetDedicatedSyncIntegrationTest extends IntegrationTestWithIsolatedEnvironment {

  override def environmentDefinition: SpliceEnvironmentDefinition =
    EnvironmentDefinition
      .fromResources(Seq("localnet-dedicated-sync-topology.conf"), this.getClass.getSimpleName)
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(Set("app-provider"))
      )
      .withManualStart

  // These do nothing as the clients will not actually be connected to the compose setup.
  override protected def runTokenStandardCliSanityCheck: Boolean = false
  override lazy val resetRequiredTopologyState = false

  // The user all localnet nodes use for their ledger API access, see
  // cluster/compose/localnet/env/*-auth-on.env
  private val ledgerApiUserId = "ledger-api-user"

  private val token = AuthUtil.testToken(AuthUtil.testAudience, ledgerApiUserId, "unsafe")

  // Above the minimum top-up, and enough that a ping visibly draws it down.
  private val purchasedTraffic = 2_000_000L

  // Covers the traffic fee with a wide margin, the fee itself depends on the amulet price.
  private val tapAmount = 100_000.0

  // LocalNet runs the apps at their default polling interval, so a change takes a few rounds of
  // automation to land.
  private val automationTimeout = 2.minutes

  private def withLocalNet(
      additionalArgs: Seq[String]
  )(f: FixtureParam => Any)(implicit env: FixtureParam): Unit =
    try {
      val ret = (Seq("build-tools/splice-localnet-compose.sh", "start") ++ additionalArgs).!
      if (ret != 0) {
        fail("Failed to start docker-compose localnet with the sync operator")
      }
      f(env)
    } finally {
      (Seq("build-tools/splice-localnet-compose.sh", "stop", "-D") ++ additionalArgs).!
    }

  private def participantClient(name: String)(implicit env: FixtureParam) = {
    val remoteParticipant =
      env.participants.remote
        .find(_.name == name)
        .getOrElse(fail(s"$name participant not found"))
    new ParticipantClientReference(
      env,
      remoteParticipant.name,
      remoteParticipant.config.copy(token = Some(token)),
    )
  }

  private def synchronizerId(
      participant: ParticipantClientReference,
      alias: String,
  ): SynchronizerId =
    participant.synchronizers
      .list_connected()
      .find(_.synchronizerAlias.unwrap == alias)
      .getOrElse(fail(s"${participant.name} is not connected to $alias"))
      .synchronizerId

  "the sync operator serves the app-synchronizer as a dedicated synchronizer" in { implicit env =>
    withLocalNet(Seq("-O")) { implicit env =>
      val participant = participantClient("app-provider")
      val appSynchronizerId = synchronizerId(participant, "app-synchronizer")

      def trafficControl() =
        participant.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(appSynchronizerId)
          .trafficControl

      def trafficState() = participant.traffic_control.traffic_state(appSynchronizerId)

      clue("the synchronizer is bootstrapped with traffic control and a base rate to join on") {
        trafficControl().value.maxBaseTrafficAmount.value should be > 0L
      }

      // The operator acts as the primary party of its ledger API user, onboarded by the
      // app-provider validator.
      val operatorParty = eventuallySucceeds(automationTimeout) {
        participant.ledger_api.users.get("sync-operator").primaryParty.value
      }

      val sv = sv_client("svClient").copy(token = Some(token))
      // The SV serves its DSO info only once it is onboarded, which trails the compose start.
      val svParty = eventuallySucceeds(automationTimeout)(sv.getDsoInfo().svParty)

      actAndCheck(automationTimeout)(
        "the DSO registers the synchronizer to the operator, which is what lets the operator act",
        sv.createVoteRequest(
          svParty.toProtoPrimitive,
          new ARC_DsoRules(
            new SRARC_RegisterSynchronizer(
              new DsoRules_RegisterSynchronizer(
                appSynchronizerId.toProtoPrimitive,
                operatorParty.toProtoPrimitive,
              )
            )
          ),
          "https://localnet.example/dedicated-sync",
          "Register the app-synchronizer as a dedicated synchronizer",
          new RelTime(Duration.ofDays(1).toMillis * 1000),
          None,
        ),
      )(
        "the operator narrows the base rate to zero, leaving traffic control on",
        _ => trafficControl().value.maxBaseTrafficAmount shouldBe NonNegativeLong.zero,
      )

      clue("with a zero base rate the participant has no allowance of its own") {
        eventually(automationTimeout) {
          val state = trafficState()
          state.extraTrafficPurchased shouldBe NonNegativeLong.zero
          state.baseTrafficRemainder shouldBe NonNegativeLong.zero
        }
      }

      clue("with nothing bought, the participant cannot transact on the dedicated synchronizer") {
        assertThrowsAndLogsCommandFailures(
          participant.health.ping(participant.id, synchronizerId = Some(appSynchronizerId)),
          _.errorMessage should include("ping"),
        )
      }

      // The registration is what authorizes a purchase on this synchronizer, and Scan is the
      // only source of its created-event blob.
      val registration = eventually(automationTimeout) {
        scancl("scanClient")
          .lookupSynchronizerRegistration(appSynchronizerId.toProtoPrimitive)
          .value
      }
      val buyer = vc("providerValidatorClient").copy(token = Some(token)).getValidatorPartyId()

      actAndCheck(automationTimeout)(
        "the participant buys traffic for the dedicated synchronizer on the global synchronizer",
        buyTraffic(participant, buyer, appSynchronizerId, registration),
      )(
        "the operator grants it on the dedicated synchronizer's sequencer",
        _ => trafficState().extraTrafficPurchased.value shouldBe purchasedTraffic,
      )

      clue("the purchased traffic is drawn down by transacting on the dedicated synchronizer") {
        val before = trafficState().extraTrafficConsumed.value
        participant.health.ping(participant.id, synchronizerId = Some(appSynchronizerId))
        eventually(automationTimeout) {
          trafficState().extraTrafficConsumed.value should be > before
        }
      }
    }
  }

  private def buyTraffic(
      participant: ParticipantClientReference,
      buyer: PartyId,
      synchronizerId: SynchronizerId,
      registration: ContractWithState[RegisteredSynchronizer.ContractId, RegisteredSynchronizer],
  )(implicit env: FixtureParam): Unit = {
    val scan = scancl("scanClient")
    val transferContext = scan.getTransferContextWithInstances(env.environment.clock.now)
    val amuletRules = transferContext.amuletRules
    val openMiningRound = transferContext.latestOpenMiningRound

    val amulet = participant.ledger_api_extensions.commands
      .submitWithResult(
        ledgerApiUserId,
        actAs = Seq(buyer),
        readAs = Seq(buyer),
        update = amuletRules.contract.contractId.exerciseAmuletRules_DevNet_Tap(
          buyer.toProtoPrimitive,
          BigDecimal(tapAmount).bigDecimal,
          openMiningRound.contract.contractId,
        ),
        disclosedContracts = DisclosedContracts
          .forTesting(amuletRules, openMiningRound)
          .toLedgerApiDisclosedContracts,
      )
      .exerciseResult
      .amuletSum
      .amulet

    val _ = participant.ledger_api_extensions.commands.submitWithResult(
      ledgerApiUserId,
      actAs = Seq(buyer),
      readAs = Seq(buyer),
      update = amuletRules.contract.contractId.exerciseAmuletRules_BuyMemberTraffic(
        Seq[splice.amuletrules.TransferInput](
          new splice.amuletrules.transferinput.InputAmulet(amulet)
        ).asJava,
        new splice.amuletrules.TransferContext(
          openMiningRound.contract.contractId,
          Map.empty[Round, IssuingMiningRound.ContractId].asJava,
          Map.empty[String, splice.amulet.ValidatorRight.ContractId].asJava,
          None.toJava,
        ),
        buyer.toProtoPrimitive,
        participant.id.toProtoPrimitive,
        synchronizerId.toProtoPrimitive,
        // a registered synchronizer is pinned to migration id 0
        0L,
        purchasedTraffic,
        Some(scan.getDsoPartyId().toProtoPrimitive).toJava,
        Some(registration.contractId).toJava,
      ),
      disclosedContracts = DisclosedContracts
        .forTesting(amuletRules, openMiningRound, registration)
        .toLedgerApiDisclosedContracts,
    )
  }
}
