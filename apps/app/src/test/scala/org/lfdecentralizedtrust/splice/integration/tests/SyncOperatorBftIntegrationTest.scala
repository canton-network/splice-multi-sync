// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.{HasExecutionContext, SequencerAlias, SynchronizerAlias}
import com.digitalasset.canton.admin.api.client.data.{
  StaticSynchronizerParameters,
  SubmissionRequestAmplification,
  TrafficControlParameters,
}
import com.digitalasset.canton.config.{
  CryptoConfig,
  FullClientConfig,
  PositiveFiniteDuration,
  SequencerApiClientConfig,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, Port, PositiveInt}
import com.digitalasset.canton.console.ConsoleMacros
import com.digitalasset.canton.synchronizer.mediator.RemoteMediatorConfig
import com.digitalasset.canton.synchronizer.sequencer.config.RemoteSequencerConfig
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.version.ProtocolVersion
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.GovernanceParameters
import org.lfdecentralizedtrust.splice.config.Thresholds
import org.lfdecentralizedtrust.splice.console.{MediatorClientReference, SequencerClientReference}
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.{
  IntegrationTest,
  SpliceTestConsoleEnvironment,
}
import org.lfdecentralizedtrust.splice.util.{
  StandaloneCanton,
  SyncOperatorTestUtil,
  WalletTestUtil,
}
import org.lfdecentralizedtrust.splice.validator.config.ValidatorAppBackendConfig

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Runs a dedicated synchronizer on four BFT nodes with one sync operator per node, and checks
  * that every node grants a member's purchased traffic and that the nodes agree on what it
  * consumes.
  */
class SyncOperatorBftIntegrationTest
    extends IntegrationTest
    with HasExecutionContext
    with StandaloneCanton
    with SyncOperatorTestUtil
    with WalletTestUtil {

  override def dbsSuffix = "sync_operator_bft"

  // The nodes' databases, see standalone-sync-operator-bft.conf, and the stores of operators 2 to
  // 4, see sync-operator-bft-topology.conf.
  override def usesDbs: Seq[String] =
    (1 to 4).flatMap(n =>
      Seq(
        s"sequencer_${dbsSuffix}_$n",
        s"sequencer_driver_${dbsSuffix}_$n",
        s"mediator_${dbsSuffix}_$n",
      )
    ) ++ (2 to 4).map(n => s"splice_apps_${dbsSuffix}_$n") ++ super.usesDbs

  private val purchase = 2_000_000L
  private val synchronizerAlias = SynchronizerAlias.tryCreate("syncOperatorBft")

  // Canton's defaults apart from a zero base rate, as bootstrap-canton.sc sets for splitwell.
  private val trafficControl = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeLong.zero,
    readVsWriteScalingFactor = PositiveInt.tryCreate(200),
    maxBaseTrafficAccumulationDuration = PositiveFiniteDuration.ofMinutes(10),
    setBalanceRequestSubmissionWindowSize = PositiveFiniteDuration.ofMinutes(5),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.zero,
    freeConfirmationResponses = false,
  )

  override def environmentDefinition: SpliceEnvironmentDefinition =
    EnvironmentDefinition
      .fromResources(
        Seq(
          "simple-topology-1sv.conf",
          "sync-operator-topology.conf",
          "sync-operator-bft-topology.conf",
        ),
        this.getClass.getSimpleName,
      )
      .withStandardSetup
      // The operators can only start once the test has bootstrapped their synchronizer.
      .withManualStart

  "sync operators" should {

    "run a dedicated synchronizer on four BFT nodes" in { implicit env =>
      initDsoWithSv1Only()
      startAllSync(aliceValidatorBackend, splitwellValidatorBackend)
      withCanton(
        Seq(testResourcesPath / "standalone-sync-operator-bft.conf"),
        Seq.empty,
        "sync-operator-bft",
      ) {
        val nodes = (1 to 4).map(node)

        val synchronizerId = clue("the four nodes bootstrap the synchronizer") {
          eventually(3.minutes) {
            forAll(nodes) { n =>
              n.sequencer.health.is_ready_for_initialization() shouldBe true
              n.mediator.health.is_ready_for_initialization() shouldBe true
            }
          }
          bootstrap(nodes)
        }

        clue("the four sequencers order together") {
          eventually() {
            forAll(nodes)(
              _.sequencer.bft
                .get_ordering_topology()
                .sequencerIds should contain theSameElementsAs nodes.map(_.sequencer.id)
            )
          }
        }

        val participant = aliceValidatorBackend.participantClientWithAdminToken
        val member = participant.id
        // Joining vets Canton's admin workflows on the synchronizer, which a member without traffic
        // could not, so the participant joins before traffic control is on.
        clue("the participant connects to all four nodes, as a validator does on the global synchronizer") {
          participant.synchronizers.connect_bft(
            nodes.map(n =>
              n.sequencer.sequencerConnection.withAlias(SequencerAlias.tryCreate(n.sequencer.name))
            ),
            synchronizerAlias,
            sequencerTrustThreshold = Thresholds.sequencerConnectionsSizeThreshold(nodes.size),
            sequencerLivenessMargin = Thresholds.sequencerConnectionsLivenessMargin(nodes.size),
            submissionRequestAmplification = SubmissionRequestAmplification(
              Thresholds.sequencerSubmissionRequestAmplification(nodes.size),
              ValidatorAppBackendConfig.DefaultSequencerRequestAmplificationPatience,
            ),
          )
        }

        clue("the owners turn on traffic control, which the operators require") {
          enableTrafficControl(nodes, synchronizerId)
        }

        val operators = syncOperatorBackend +: (2 to 4).map(n => syncop(s"syncOperator$n"))
        startAllSync(operators*)

        clue("the operators grant the mediators unlimited traffic") {
          eventually(1.minute) {
            forAll(nodes)(n =>
              extraTrafficLimit(n.mediator.id) shouldBe NonNegativeLong.maxValue.value
            )
          }
        }

        val operatorParty = syncOperatorBackend.appState.store.key.operatorParty
        val dsoParty = sv1Backend.getDsoInfo().dsoParty
        val dsoRules = sv1Backend.getDsoInfo().dsoRules
        clue("the DSO registers the synchronizer to the operator") {
          sv1Backend.participantClientWithAdminToken.ledger_api_extensions.commands
            .submitJava(
              actAs = Seq(dsoParty),
              readAs = Seq(dsoParty),
              commands = dsoRules.contractId
                .exerciseDsoRules_RegisterSynchronizer(
                  synchronizerId.toProtoPrimitive,
                  operatorParty.toProtoPrimitive,
                  new GovernanceParameters(java.math.BigDecimal.ONE.setScale(10)),
                )
                .commands
                .asScala
                .toSeq,
              userId = sv1Backend.config.ledgerApiUser,
            )
        }
        val registration = eventually() {
          sv1ScanBackend.lookupSynchronizerRegistration(synchronizerId.toProtoPrimitive).value
        }

        // Alice's party is pushed to every synchronizer the participant is connected to, and this
        // one refuses it until the participant has traffic.
        val aliceParty = onboardWalletUser(aliceWalletClient, aliceValidatorBackend)
        aliceWalletClient.tap(walletUsdToAmulet(200.0))

        // The participant's purchased and consumed traffic as each node reports it.
        def trafficOnEachNode() =
          nodes.map(
            _.sequencer.traffic_control
              .traffic_state_of_members(Seq(member))
              .trafficStates
              .get(member)
              .map(state => (state.extraTrafficPurchased.value, state.extraTrafficConsumed.value))
          )

        actAndCheck(1.minute)(
          "alice buys traffic for her participant on the synchronizer",
          buyTraffic(aliceParty, member, synchronizerId, Some(registration), dsoParty, purchase),
        )(
          "every node grants it",
          _ => trafficOnEachNode().map(_.map(_._1)) shouldBe Seq.fill(nodes.size)(Some(purchase)),
        )

        // Once granted, the refused push goes through and consumes the purchased traffic.
        clue("the nodes agree on the traffic the participant consumes") {
          eventually(1.minute) {
            val (purchased, consumed) = trafficOnEachNode().distinct.loneElement.value
            purchased shouldBe purchase
            consumed should be > 0L
          }
        }

        clue("stop apps manually to prevent errors from the synchronizer being force stopped") {
          stopAllAsync(operators*).futureValue
          participant.synchronizers.disconnect(synchronizerAlias)
        }
      }
    }
  }

  // A node of the synchronizer. Node n uses ports 29n07 to 29n10, see
  // standalone-sync-operator-bft.conf.
  private case class Node(sequencer: SequencerClientReference, mediator: MediatorClientReference)

  private def node(n: Int)(implicit env: SpliceTestConsoleEnvironment): Node = {
    val base = 29000 + n * 100
    Node(
      new SequencerClientReference(
        env,
        s"syncOperatorBftSequencer$n",
        RemoteSequencerConfig(
          adminApi = FullClientConfig(port = Port.tryCreate(base + 9)),
          publicApi = SequencerApiClientConfig("localhost", Port.tryCreate(base + 8)),
        ),
      ),
      new MediatorClientReference(
        env,
        s"syncOperatorBftMediator$n",
        RemoteMediatorConfig(adminApi = FullClientConfig(port = Port.tryCreate(base + 7))),
      ),
    )
  }

  // The owners that sign topology changes, as many as the threshold the synchronizer is
  // bootstrapped with.
  private def signers(nodes: Seq[Node]): Seq[SequencerClientReference] =
    nodes.map(_.sequencer).take(Thresholds.decentralizedNamespaceThreshold(nodes.size).value)

  // Bootstraps the synchronizer with the thresholds Splice sets for the global synchronizer.
  private def bootstrap(
      nodes: Seq[Node]
  )(implicit env: SpliceTestConsoleEnvironment): SynchronizerId = {
    val sequencers = nodes.map(_.sequencer)
    val sequencerThreshold = Thresholds.sequencerConnectionsSizeThreshold(sequencers.size)
    val synchronizerId = ConsoleMacros.bootstrap
      .synchronizer(
        "syncOperatorBft",
        sequencers = sequencers,
        mediators = nodes.map(_.mediator),
        synchronizerOwners = sequencers,
        synchronizerThreshold = Thresholds.decentralizedNamespaceThreshold(sequencers.size),
        staticSynchronizerParameters =
          StaticSynchronizerParameters.defaults(CryptoConfig(), ProtocolVersion.v35),
        mediatorThreshold = Thresholds.mediatorDomainStateThreshold(nodes.size),
      )
      .logical
    signers(nodes).foreach(owner =>
      owner.topology.sequencers.propose(
        synchronizerId,
        threshold = sequencerThreshold,
        active = sequencers.map(_.id),
        signedBy = Some(owner.id.uid.namespace.fingerprint),
        synchronize = None,
      )
    )
    eventually(1.minute) {
      sequencers.head.topology.sequencers
        .list(store = Some(TopologyStoreId.Synchronizer(synchronizerId)))
        .loneElement
        .item
        .threshold shouldBe sequencerThreshold
    }
    synchronizerId
  }

  private def enableTrafficControl(nodes: Seq[Node], synchronizerId: SynchronizerId): Unit = {
    signers(nodes).foreach(owner =>
      owner.topology.synchronizer_parameters.propose_update(
        synchronizerId,
        _.update(trafficControl = Some(trafficControl)),
        signedBy = Some(owner.id.uid.namespace.fingerprint),
        synchronize = None,
      )
    )
    eventually(1.minute) {
      nodes.head.sequencer.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .trafficControl should not be empty
    }
  }
}
