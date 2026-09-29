package org.lfdecentralizedtrust.splice.integration.tests

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.SequencerConnections
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.config.{
  FullClientConfig,
  NonNegativeDuration,
  SequencerApiClientConfig,
}
import com.digitalasset.canton.console.{ConsoleMacros, InstanceReference}
import com.digitalasset.canton.crypto.SigningKeyUsage.{Protocol, SequencerAuthentication}
import com.digitalasset.canton.participant.config.RemoteParticipantConfig
import com.digitalasset.canton.synchronizer.mediator.RemoteMediatorConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.config.RemoteSequencerConfig
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  ParticipantPermission,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, PartyId, SynchronizerId}
import org.lfdecentralizedtrust.splice.auth.AuthUtil
import org.lfdecentralizedtrust.splice.automation.GrantUnlimitedTrafficTriggerBase.UnlimitedTraffic
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.MemberTraffic
import org.lfdecentralizedtrust.splice.config.Thresholds
import org.lfdecentralizedtrust.splice.console.{
  MediatorClientReference,
  ParticipantClientReference,
  SequencerClientReference,
}

import java.nio.file.Files
import scala.concurrent.duration.*

/** Verifies that the app-synchronizer runs on four BFT nodes, each run by its own org, that agree on
  * every member's traffic: the four orgs create the operator party together, purchases and
  * consumption match on all nodes and orgs, a fifth node joins with the same amounts and serves a
  * participant while its org joins the party, and the synchronizer keeps ordering with one of four
  * nodes down but not with two.
  *
  * This spins up the docker-compose localnet with the four-node app-synchronizer (-B)
  */
class LocalNetDedicatedSyncBftIntegrationTest extends LocalNetDedicatedSyncIntegrationTestBase {

  // A node of the app-synchronizer and the participant of the org that runs it.
  private case class Node(
      sequencer: SequencerClientReference,
      mediator: MediatorClientReference,
      p2pPort: Int,
      orgParticipant: ParticipantClientReference,
  )

  private type PeerLink = (SequencerClientReference, BftBlockOrdererConfig.P2PEndpointConfig)

  // A stuck synchronizer backs its view changes off to 30 seconds, so recovery gets a wider window.
  private val recoveryTimeout = 5.minutes

  // The user each org's sync operator reads as, see conf/console/app-synchronizer.sc.
  private val operatorUser = "sync-operator"

  private val globalAlias = SynchronizerAlias.tryCreate("global")

  // Node 1 uses ports 50xx and node n ports 5nxx, and the participants of their orgs 60xx and 6nxx,
  // see conf/canton/app-synchronizer-bft.
  private def node(n: Int)(implicit env: FixtureParam): Node = {
    val base = if (n == 1) 5000 else 5000 + n * 100
    val orgBase = base + 1000
    val suffix = if (n == 1) "" else s"-$n"
    Node(
      new SequencerClientReference(
        env,
        s"app-sequencer$suffix",
        RemoteSequencerConfig(
          adminApi = FullClientConfig("localhost", Port.tryCreate(base + 19)),
          // Reachable from the test and, as all nodes run in the canton container, from the nodes.
          publicApi = SequencerApiClientConfig("localhost", Port.tryCreate(base + 18)),
        ),
      ),
      new MediatorClientReference(
        env,
        s"app-mediator$suffix",
        RemoteMediatorConfig(adminApi = FullClientConfig("localhost", Port.tryCreate(base + 17))),
      ),
      p2pPort = base + 10,
      orgParticipant = new ParticipantClientReference(
        env,
        s"app-operator$suffix",
        RemoteParticipantConfig(
          adminApi = FullClientConfig("localhost", Port.tryCreate(orgBase + 2)),
          ledgerApi = FullClientConfig("localhost", Port.tryCreate(orgBase + 1)),
          token = Some(token),
        ),
      ),
    )
  }

  // The org's participant as the org's sync operator sees it.
  private def asOperator(participant: ParticipantClientReference)(implicit env: FixtureParam) =
    new ParticipantClientReference(
      env,
      participant.name,
      participant.config
        .copy(token = Some(AuthUtil.testToken(AuthUtil.testAudience, operatorUser, "unsafe"))),
    )

  "the four-node app-synchronizer keeps traffic consistent as nodes and orgs join, leave and go down" in {
    implicit env =>
      withLocalNet(Seq("-B")) { implicit env =>
        val participant = participantClient("app-provider")
        val appSynchronizerAlias = SynchronizerAlias.tryCreate("app-synchronizer")
        val appSynchronizerId = synchronizerId(participant, "app-synchronizer")
        val store = TopologyStoreId.Synchronizer(appSynchronizerId)
        val nodes = (1 to 4).map(n => node(n))
        val joiner = node(5)
        // A topology change takes effect once three of the four owners have signed it.
        val signers = nodes.take(3).map(_.sequencer)
        val operator = operatorParty(nodes.head.orgParticipant)
        val globalSynchronizerId = synchronizerId(nodes.head.orgParticipant, "global")

        def sequencerState() =
          participant.topology.sequencers.list(store = Some(store)).loneElement.item

        def mediatorState() =
          participant.topology.mediators
            .list(Some(appSynchronizerId), group = Some(NonNegativeInt.zero))
            .loneElement
            .item

        def ping() =
          participant.health.ping(participant.id, synchronizerId = Some(appSynchronizerId))

        // A connected participant only picks up changed sequencer connections on reconnect.
        def useSequencers(connections: SequencerConnections): Unit = {
          participant.synchronizers.disconnect(appSynchronizerAlias)
          participant.synchronizers.modify(
            appSynchronizerAlias,
            _.copy(sequencerConnections = connections),
          )
          val _ = participant.synchronizers.reconnect(appSynchronizerAlias)
        }

        def trafficOn(n: Node) =
          n.sequencer.traffic_control
            .traffic_state_of_members(Seq(participant.id))
            .trafficStates
            .get(participant.id)

        // The participant's purchased and consumed traffic as each node reports it.
        def trafficOnAll(on: Seq[Node]) =
          on.map(
            trafficOn(_).map(state =>
              (state.extraTrafficPurchased.value, state.extraTrafficConsumed.value)
            )
          )

        def operatorNamespace() =
          decentralizedNamespace(nodes.head.orgParticipant, operator, globalSynchronizerId)

        def operatorHosting() =
          partyHosting(nodes.head.orgParticipant, operator, globalSynchronizerId)

        // The traffic bought for the participant, as each org's sync operator reads it from the
        // org's own participant.
        def purchasesSeenBy(orgs: Seq[Node]) =
          orgs.map(n =>
            asOperator(n.orgParticipant).ledger_api_extensions.acs
              .filterJava(MemberTraffic.COMPANION)(
                operator,
                traffic =>
                  traffic.data.memberId == participant.id.toProtoPrimitive &&
                    traffic.data.synchronizerId == appSynchronizerId.toProtoPrimitive,
              )
              .map(_.data.totalPurchased.longValue)
              .sum
          )

        // Every node reports the given purchased total and one consumed amount above the given one,
        // which it returns.
        def trafficAgrees(
            on: Seq[Node],
            purchased: Long,
            consumedAbove: Long,
            timeout: FiniteDuration = automationTimeout,
        ): Long =
          eventuallySucceeds(timeout) {
            val (purchasedOnAll, consumedOnAll) = trafficOnAll(on).distinct.loneElement.value
            purchasedOnAll shouldBe purchased
            consumedOnAll should be > consumedAbove
            consumedOnAll
          }

        clue("four nodes run the synchronizer, and their sequencers own it together") {
          sequencerState().active.forgetNE should contain theSameElementsAs nodes.map(
            _.sequencer.id
          )
          sequencerState().threshold shouldBe PositiveInt.two
          mediatorState().active.forgetNE should contain theSameElementsAs nodes.map(
            _.mediator.id
          )
          mediatorState().threshold shouldBe PositiveInt.two
          val namespace = participant.topology.decentralized_namespaces
            .list(store, filterNamespace = appSynchronizerId.uid.namespace.toProtoPrimitive)
            .loneElement
            .item
          namespace.owners.forgetNE should contain theSameElementsAs nodes.map(
            _.sequencer.id.uid.namespace
          )
          namespace.threshold shouldBe PositiveInt.three
          nodes.foreach(n =>
            n.sequencer.bft
              .get_ordering_topology()
              .sequencerIds should contain theSameElementsAs nodes.map(_.sequencer.id)
          )
        }

        clue("the participant reads from and submits to several sequencers") {
          val connections = participant.synchronizers
            .config(appSynchronizerAlias)
            .value
            .sequencerConnections
          connections.connections.forgetNE should have size 4
          connections.sequencerTrustThreshold shouldBe PositiveInt.two
        }

        clue("the four orgs create the operator party together and host it on their participants") {
          operatorNamespace().owners.forgetNE should contain theSameElementsAs nodes.map(
            _.orgParticipant.id.uid.namespace
          )
          operatorNamespace().threshold shouldBe PositiveInt.three
          operatorHosting().participants.map(host =>
            host.participantId -> host.permission
          ) should contain theSameElementsAs nodes.map(
            _.orgParticipant.id -> ParticipantPermission.Confirmation
          )
          nodes.foreach(n => operatorParty(n.orgParticipant) shouldBe operator)
        }

        val registration = registerSynchronizer(appSynchronizerId, operator)
        val buyer = vc("providerValidatorClient").copy(token = Some(token)).getValidatorPartyId()

        // Buys traffic and spends some of it. Once the given orgs all read the same purchases, returns
        // the consumption the given nodes agree on.
        def buyAndSpend(
            on: Seq[Node],
            orgs: Seq[Node],
            purchased: Long,
            consumedBefore: Long,
        ): Long = {
          actAndCheck(automationTimeout)(
            "the participant buys traffic",
            buyTraffic(participant, buyer, appSynchronizerId, registration),
          )(
            "the operators grant it",
            _ => trafficOn(on.head).map(_.extraTrafficPurchased.value) shouldBe Some(purchased),
          )
          eventuallySucceeds(automationTimeout) {
            purchasesSeenBy(orgs).distinct shouldBe Seq(purchased)
          }
          eventuallySucceeds(automationTimeout)(ping())
          trafficAgrees(on, purchased, consumedBefore)
        }

        val consumedOnFour = clue("purchase and consumption agree on all four nodes and orgs") {
          buyAndSpend(nodes, nodes, purchasedTraffic, 0L)
        }

        actAndCheck(automationTimeout)(
          "node 5 joins the running synchronizer", {
            ConsoleMacros.bootstrap.onboard_new_sequencer(
              appSynchronizerId,
              joiner.sequencer,
              nodes.head.sequencer,
              signers.toSet[InstanceReference],
              customCommandTimeout = Some(NonNegativeDuration.tryFromDuration(automationTimeout)),
              isBftSequencer = true,
            )
            joiner.sequencer.health.wait_for_initialized()
            addMediator(joiner, nodes.head.sequencer, signers, appSynchronizerId)
          },
        )(
          "node 5 is in every node's ordering topology and its mediator is granted traffic",
          _ => {
            (nodes :+ joiner).foreach(n =>
              n.sequencer.bft.get_ordering_topology().sequencerIds should contain(
                joiner.sequencer.id
              )
            )
            nodes.head.sequencer.traffic_control
              .traffic_state_of_members(Seq(joiner.mediator.id))
              .trafficStates
              .get(joiner.mediator.id)
              .map(_.extraTrafficPurchased) shouldBe Some(UnlimitedTraffic)
          },
        )

        clue("node 5 starts with the traffic the other nodes hold") {
          trafficAgrees(nodes :+ joiner, purchasedTraffic, 0L)
        }

        actAndCheck(automationTimeout)(
          "org 5 joins the operator party",
          addOrg(joiner, nodes, operator, globalSynchronizerId),
        )(
          "org 5 hosts and co-owns the party, and reads the purchases the other orgs read",
          _ => {
            operatorHosting().participants
              .find(_.participantId == joiner.orgParticipant.id)
              .map(_.onboarding) shouldBe Some(false)
            operatorNamespace().owners.forgetNE should contain(
              joiner.orgParticipant.id.uid.namespace
            )
            purchasesSeenBy(nodes :+ joiner).distinct shouldBe Seq(purchasedTraffic)
          },
        )

        val consumedThroughJoiner =
          clue("traffic bought and spent through node 5 alone agrees on all five nodes and orgs") {
            val connections = participant.synchronizers
              .config(appSynchronizerAlias)
              .value
              .sequencerConnections
            useSequencers(SequencerConnections.single(joiner.sequencer.sequencerConnection))
            val consumed =
              buyAndSpend(nodes :+ joiner, nodes :+ joiner, 2 * purchasedTraffic, consumedOnFour)
            useSequencers(connections)
            consumed
          }

        actAndCheck(automationTimeout)(
          "node 5 leaves the synchronizer",
          removeNode(joiner, nodes.map(_.sequencer), signers, appSynchronizerId),
        )(
          "every remaining node drops node 5 from its ordering topology",
          _ => {
            nodes.foreach(n =>
              n.sequencer.bft
                .get_ordering_topology()
                .sequencerIds should contain theSameElementsAs nodes.map(_.sequencer.id)
            )
            mediatorState().active.forgetNE should not contain joiner.mediator.id
          },
        )

        actAndCheck(automationTimeout)(
          "org 5 leaves the operator party",
          removeOrg(joiner, nodes, operator, globalSynchronizerId),
        )(
          "the four orgs host and own the party again",
          _ => {
            operatorHosting().participants
              .map(_.participantId) should contain theSameElementsAs nodes
              .map(_.orgParticipant.id)
            operatorNamespace().owners.forgetNE should contain theSameElementsAs nodes.map(
              _.orgParticipant.id.uid.namespace
            )
            operatorNamespace().threshold shouldBe PositiveInt.three
          },
        )

        val node4Links = clue("node 4 is cut off from the other nodes")(isolate(nodes(3), nodes))

        val consumedWithOneDown =
          clue("with node 4 down, the other three grant and sequence traffic and agree on it") {
            buyAndSpend(nodes.take(3), nodes, 3 * purchasedTraffic, consumedThroughJoiner)
          }

        val node3Links = clue("node 3 is cut off as well")(isolate(nodes(2), nodes))

        clue("with two of the four nodes down, the synchronizer stops ordering") {
          assertThrowsAndLogsCommandFailures(ping(), _.errorMessage should include("ping"))
        }

        clue("once nodes 3 and 4 are back, all four nodes catch up and agree") {
          restore(node3Links ++ node4Links)
          eventuallySucceeds(recoveryTimeout)(ping())
          trafficAgrees(nodes, 3 * purchasedTraffic, consumedWithOneDown, recoveryTimeout)
        }
      }
  }

  // Follows Canton's documented mediator onboarding. The console only has a macro for sequencers.
  private def addMediator(
      joiner: Node,
      existing: SequencerClientReference,
      signers: Seq[SequencerClientReference],
      synchronizerId: SynchronizerId,
  ): Unit = {
    val store = TopologyStoreId.Synchronizer(synchronizerId)
    existing.topology.transactions.load(
      joiner.mediator.topology.transactions.identity_transactions(),
      store,
      ForceFlags(ForceFlag.AlienMember),
    )
    // The group only admits a mediator whose keys the synchronizer already knows.
    eventually(automationTimeout) {
      existing.topology.transactions
        .list(
          store = store,
          filterNamespace = joiner.mediator.namespace.filterString,
          filterMappings = Seq(
            TopologyMapping.Code.NamespaceDelegation,
            TopologyMapping.Code.OwnerToKeyMapping,
          ),
        )
        .result should have size 2
    }
    val group = mediatorGroup(existing, synchronizerId)
    signers.foreach(
      _.topology.mediators.propose(
        synchronizerId,
        threshold = group.threshold,
        active = group.active.forgetNE :+ joiner.mediator.id,
        group = NonNegativeInt.zero,
      )
    )
    eventually(automationTimeout) {
      mediatorGroup(existing, synchronizerId).active.forgetNE should contain(joiner.mediator.id)
    }
    joiner.mediator.setup.assign(
      existing.physical_synchronizer_id,
      SequencerConnections.single(joiner.sequencer.sequencerConnection),
    )
    joiner.mediator.health.wait_for_initialized()
  }

  // Follows Canton's documented offboarding. The mediator leaves first, as it reads from the
  // sequencer that leaves.
  private def removeNode(
      leaver: Node,
      remaining: Seq[SequencerClientReference],
      signers: Seq[SequencerClientReference],
      synchronizerId: SynchronizerId,
  ): Unit = {
    val existing = signers.head
    val group = mediatorGroup(existing, synchronizerId)
    signers.foreach(
      _.topology.mediators.propose(
        synchronizerId,
        threshold = group.threshold,
        active = group.active.forgetNE.filterNot(_ == leaver.mediator.id),
        group = NonNegativeInt.zero,
      )
    )
    eventually(automationTimeout) {
      mediatorGroup(existing, synchronizerId).active.forgetNE should not contain leaver.mediator.id
    }
    // The sequencer drops the keys only it uses before the owners remove it.
    val remainingKeys = remaining.flatMap(_.keys.public.list()).map(_.publicKey)
    leaver.sequencer.keys.public
      .list(filterUsage = Set(SequencerAuthentication, Protocol))
      .map(_.publicKey)
      .diff(remainingKeys)
      .foreach(key =>
        leaver.sequencer.topology.owner_to_key_mappings.remove_key(key.fingerprint, key.purpose)
      )
    val sequencers = existing.topology.sequencers
      .list(store = Some(TopologyStoreId.Synchronizer(synchronizerId)))
      .loneElement
      .item
    signers.foreach(
      _.topology.sequencers.propose(
        synchronizerId,
        threshold = sequencers.threshold,
        active = sequencers.active.forgetNE.filterNot(_ == leaver.sequencer.id),
      )
    )
  }

  // Follows Canton's documented onboarding of a party hosted by several participants: the org's
  // participant imports the party's contracts, then the owners admit the org as a co-owner.
  private def addOrg(
      joiner: Node,
      orgs: Seq[Node],
      party: PartyId,
      synchronizerId: SynchronizerId,
  ): Unit = {
    val store = TopologyStoreId.Synchronizer(synchronizerId)
    val source = orgs.head.orgParticipant
    val target = joiner.orgParticipant
    val hosts = (orgs :+ joiner).map(_.orgParticipant)
    val threshold = partyHosting(source, party, synchronizerId).threshold
    val beginOffset = source.ledger_api.state.end()
    hosts.foreach(
      _.topology.party_to_participant_mappings.propose(
        party,
        newParticipants = hosts.map(_.id -> ParticipantPermission.Confirmation),
        threshold = threshold,
        store = store,
        participantsRequiringPartyToBeOnboarded = Seq(target.id),
      )
    )
    eventually(automationTimeout) {
      hosts.foreach(
        _.topology.party_to_participant_mappings
          .is_known(synchronizerId, party, hosts.map(_.id)) shouldBe true
      )
    }
    val acsFile = Files.createTempFile("operator-acs", ".gz")
    acsFile.toFile.deleteOnExit()
    source.parties.export_party_acs(party, synchronizerId, target.id, beginOffset, acsFile.toString)
    target.synchronizers.disconnect(globalAlias)
    target.parties.import_party_acs(synchronizerId, Some(party), acsFile.toString)
    val _ = target.synchronizers.reconnect(globalAlias)
    // The org's sync operator reads as the party, as on the other orgs' participants.
    val _ = target.ledger_api.users.create(
      operatorUser,
      primaryParty = Some(party),
      readAs = Set(party),
    )
    val namespace = decentralizedNamespace(source, party, synchronizerId)
    val owners = NonEmpty(Set, target.id.uid.namespace, namespace.owners.forgetNE.toSeq*)
    val withJoiner = DecentralizedNamespaceDefinition
      .create(
        namespace.namespace,
        Thresholds.decentralizedNamespaceThreshold(owners.size),
        owners,
      )
      .value
    hosts.foreach(host =>
      host.topology.decentralized_namespaces.propose(
        withJoiner,
        store,
        signedBy = Seq(host.id.uid.namespace.fingerprint),
      )
    )
  }

  // Follows Canton's documented offboarding of a party host: the remaining hosts drop the org's
  // participant, then the remaining owners remove the org.
  private def removeOrg(
      leaver: Node,
      orgs: Seq[Node],
      party: PartyId,
      synchronizerId: SynchronizerId,
  ): Unit = {
    val store = TopologyStoreId.Synchronizer(synchronizerId)
    val hosts = orgs.map(_.orgParticipant)
    val threshold = partyHosting(hosts.head, party, synchronizerId).threshold
    hosts.foreach(
      _.topology.party_to_participant_mappings.propose(
        party,
        newParticipants = hosts.map(_.id -> ParticipantPermission.Confirmation),
        threshold = threshold,
        store = store,
      )
    )
    eventually(automationTimeout) {
      partyHosting(hosts.head, party, synchronizerId).participants.map(
        _.participantId
      ) should not contain leaver.orgParticipant.id
    }
    val namespace = decentralizedNamespace(hosts.head, party, synchronizerId)
    val owners =
      NonEmpty.from(namespace.owners.forgetNE - leaver.orgParticipant.id.uid.namespace).value
    val withoutLeaver = DecentralizedNamespaceDefinition
      .create(
        namespace.namespace,
        Thresholds.decentralizedNamespaceThreshold(owners.size),
        owners,
      )
      .value
    hosts.foreach(host =>
      host.topology.decentralized_namespaces.propose(
        withoutLeaver,
        store,
        signedBy = Seq(host.id.uid.namespace.fingerprint),
      )
    )
  }

  // Removes every p2p link to and from the node, so the other nodes see it as down, and returns
  // the removed links.
  private def isolate(target: Node, nodes: Seq[Node]): Seq[PeerLink] = {
    val links = nodes.flatMap(n =>
      n.sequencer.bft.list_configured_peer_endpoints().collect {
        case (endpoint, _) if n == target || endpoint.port.unwrap == target.p2pPort =>
          (n.sequencer, endpoint)
      }
    )
    links.foreach { case (sequencer, endpoint) =>
      sequencer.bft.remove_peer_endpoint(
        BftBlockOrdererConfig.EndpointId(
          endpoint.address,
          endpoint.port,
          endpoint.transportSecurity,
        )
      )
    }
    links.map { case (sequencer, endpoint) => sequencer -> endpoint.endpointConfig }
  }

  private def restore(links: Seq[PeerLink]): Unit =
    links.foreach { case (sequencer, endpoint) => sequencer.bft.add_peer_endpoint(endpoint) }

  private def partyHosting(
      participant: ParticipantClientReference,
      party: PartyId,
      synchronizerId: SynchronizerId,
  ) =
    participant.topology.party_to_participant_mappings
      .list(synchronizerId, filterParty = party.toProtoPrimitive)
      .loneElement
      .item

  // The namespace that owns the party.
  private def decentralizedNamespace(
      participant: ParticipantClientReference,
      party: PartyId,
      synchronizerId: SynchronizerId,
  ) =
    participant.topology.decentralized_namespaces
      .list(
        TopologyStoreId.Synchronizer(synchronizerId),
        filterNamespace = party.uid.namespace.toProtoPrimitive,
      )
      .loneElement
      .item

  private def mediatorGroup(sequencer: SequencerClientReference, synchronizerId: SynchronizerId) =
    sequencer.topology.mediators
      .list(Some(synchronizerId), group = Some(NonNegativeInt.zero))
      .loneElement
      .item
}
