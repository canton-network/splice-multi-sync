package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import monocle.Monocle.toAppliedFocusOps
import org.lfdecentralizedtrust.splice.auth.AuthUtil
import org.lfdecentralizedtrust.splice.codegen.java.da.time.types.RelTime
import org.lfdecentralizedtrust.splice.codegen.java.splice
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.GovernanceParameters
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

/** Shared setup for tests that drive the docker-compose localnet with the sync operator enabled,
  * where the app-synchronizer plays the dedicated synchronizer.
  */
abstract class LocalNetDedicatedSyncIntegrationTestBase
    extends IntegrationTestWithIsolatedEnvironment {

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
  protected val ledgerApiUserId = "ledger-api-user"

  protected val token: String = AuthUtil.testToken(AuthUtil.testAudience, ledgerApiUserId, "unsafe")

  // Above the minimum top-up, and enough that a ping visibly draws it down.
  protected val purchasedTraffic = 2_000_000L

  // Covers the traffic fee with a wide margin, the fee itself depends on the amulet price.
  protected val tapAmount = 100_000.0

  // LocalNet runs the apps at their default polling interval, so a change takes a few rounds of
  // automation to land.
  protected val automationTimeout: FiniteDuration = 2.minutes

  protected def withLocalNet(
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

  protected def participantClient(name: String)(implicit env: FixtureParam) = {
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

  protected def synchronizerId(
      participant: ParticipantClientReference,
      alias: String,
  ): SynchronizerId =
    participant.synchronizers
      .list_connected()
      .find(_.synchronizerAlias.unwrap == alias)
      .getOrElse(fail(s"${participant.name} is not connected to $alias"))
      .synchronizerId

  /** The operator acts as the primary party of its ledger API user, onboarded by the app-provider
    * validator.
    */
  protected def operatorParty(participant: ParticipantClientReference): PartyId =
    eventuallySucceeds(automationTimeout) {
      participant.ledger_api.users.get("sync-operator").primaryParty.value
    }

  /** Puts the registration vote through the SV and returns the registration Scan then serves. */
  protected def registerSynchronizer(
      appSynchronizerId: SynchronizerId,
      operator: PartyId,
  )(implicit
      env: FixtureParam
  ): ContractWithState[RegisteredSynchronizer.ContractId, RegisteredSynchronizer] = {
    val sv = sv_client("svClient").copy(token = Some(token))
    // The SV serves its DSO info only once it is onboarded, which trails the compose start.
    val svParty = eventuallySucceeds(automationTimeout)(sv.getDsoInfo().svParty)

    clue("the DSO registers the synchronizer to the operator") {
      sv.createVoteRequest(
        svParty.toProtoPrimitive,
        new ARC_DsoRules(
          new SRARC_RegisterSynchronizer(
            new DsoRules_RegisterSynchronizer(
              appSynchronizerId.toProtoPrimitive,
              operator.toProtoPrimitive,
              new GovernanceParameters(java.math.BigDecimal.ONE.setScale(10)),
            )
          )
        ),
        "https://localnet.example/dedicated-sync",
        "Register the app-synchronizer as a dedicated synchronizer",
        new RelTime(Duration.ofDays(1).toMillis * 1000),
        None,
      )
    }

    // Scan serves the registration once the vote has closed and Scan itself is up, which
    // trails the compose start.
    eventuallySucceeds(automationTimeout) {
      scancl("scanClient")
        .lookupSynchronizerRegistration(appSynchronizerId.toProtoPrimitive)
        .value
    }
  }

  protected def buyTraffic(
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
