// Bootstraps a dedicated synchronizer that is operated by 4 nodes (a sequencer, a mediator and a participant each).
//
// This console has access to all nodes, but it only ever does what the operator of a single node could do with the
// nodes it operates, one node at a time. It does not use the `bootstrap.synchronizer` macro, which needs access to all
// nodes at once. Anything that operators would need to send each other goes through files in an exchange directory
// (DSYNC_EXCHANGE_DIR, a temporary directory by default), so that each part can be split into separate consoles per
// operator. Each phase below says what the operator of a node does in it, what it needs to have received before, and
// what it shares afterwards. Based on https://docs.canton.network/global-synchronizer/extension-synchronizers/synchronizer-operations#set-up-a-decentralized-synchronizer

import com.digitalasset.canton.console.{InstanceReference, MediatorReference, ParticipantReference, SequencerReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.protocol.{DynamicSynchronizerParameters, SynchronizerLimits}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransactionDeserializationContext
import java.nio.file.{Files, Path, Paths}

val numNodes = 4
val synchronizerName = "dedicated"
// We have a threshold of three, so signing the bootstrapping transactions only with the first three sequencers.
val numSigningSequencers = 3
// The sequencer threshold set below: only this many sequencers need to submit each traffic update.
val numTrafficSequencers = 2

case class DsyncNode(
    index: Int,
    sequencer: SequencerReference,
    mediator: MediatorReference,
    participant: ParticipantReference,
)

def remote[T <: InstanceReference](refs: Seq[T], name: String): T =
  refs.find(_.name == name).getOrElse(sys.error(s"Node $name is not configured in this console"))

// The names of the nodes determine their identities.
val nodes = (1 to numNodes).map(i =>
  DsyncNode(
    i,
    remote(sequencers.remote, s"sequencer$i"),
    remote(mediators.remote, s"mediator$i"),
    remote(participants.remote, s"participant$i"),
  )
)
val signingNodes = nodes.take(numSigningSequencers)
val trafficNodes = nodes.take(numTrafficSequencers)

// ---------------------------------------------------------------------------------------------------------------
// Exchanging files
// ---------------------------------------------------------------------------------------------------------------

val exchangeDir: Path = sys.env.get("DSYNC_EXCHANGE_DIR") match {
  case Some(dir) => Files.createDirectories(Paths.get(dir))
  case None => Files.createTempDirectory("dsync-bootstrap")
}
println(s"Exchanging files between the nodes in $exchangeDir")

def exchangeFile(name: String): String = exchangeDir.resolve(name).toString

def writeTransactions(
    name: String,
    txs: Seq[GenericSignedTopologyTransaction],
    protocolVersion: ProtocolVersion,
): Unit = {
  Files.write(
    exchangeDir.resolve(name),
    SignedTopologyTransactions(txs, protocolVersion).toByteString.toByteArray,
  )
  ()
}

def readTransactions(name: String): Seq[GenericSignedTopologyTransaction] =
  SignedTopologyTransactions
    .readFromTrustedFile(
      TopologyTransactionDeserializationContext(SynchronizerLimits.default),
      exchangeFile(name),
    )
    .fold(err => sys.error(s"Unable to read $name: $err"), _.transactions)

// ---------------------------------------------------------------------------------------------------------------
// Initializing the nodes
// ---------------------------------------------------------------------------------------------------------------

def getOrGenerateSigningKey(node: InstanceReference, usage: SigningKeyUsage): SigningPublicKey = {
  val name = node.name + s"-${usage.identifier}"
  node.keys.secret
    .list(filterName = name)
    .filter(_.publicKeyWithName.name.map(_.unwrap).contains(name))
    .map(_.publicKeyWithName.publicKey)
    .collectFirst { case key: SigningPublicKey => key } match {
    case Some(key) =>
      println(s"Key $name already exists, skipping")
      key
    case None =>
      println(s"Generating key $name...")
      node.keys.secret.generate_signing_key(name = name, usage = Set(usage))
  }
}

def initManually(node: InstanceReference): Unit =
  if (node.health.initialized()) {
    println("Node " + node.name + " is already initialized, skipping")
  } else {
    val namespaceKey = getOrGenerateSigningKey(node, SigningKeyUsage.Namespace)
    val sequencerAuthKey = getOrGenerateSigningKey(node, SigningKeyUsage.SequencerAuthentication)
    val signingKey = getOrGenerateSigningKey(node, SigningKeyUsage.Protocol)
    val namespace = Namespace(namespaceKey.id)

    if (node.health.has_identity()) {
      println("Node " + node.name + " already has an identity, skipping")
    } else {
      println("Bootstrapping node " + node.name + "...")
      node.topology.init_id_from_uid(
        UniqueIdentifier.tryCreate(node.name, namespace)
      )
    }

    utils.retry_until_true(
      node.health.is_ready_for_node_topology() || node.health.is_ready_for_initialization()
    )

    if (
      node.topology.namespace_delegations
        .list(store = TopologyStoreId.Authorized, filterNamespace = namespace.toProtoPrimitive)
        .nonEmpty
    ) {
      println("Namespace delegation for node " + node.name + " already exists, skipping")
    } else {
      println("Proposing namespace delegation for node " + node.name + "...")
      node.topology.namespace_delegations.propose_delegation(
        namespace,
        namespaceKey,
        CanSignAllMappings,
      )
    }

    if (
      node.topology.owner_to_key_mappings
        .list(
          store = Some(TopologyStoreId.Authorized),
          filterKeyOwnerUid = node.id.uid.toProtoPrimitive,
        )
        .nonEmpty
    ) {
      println("Owner to key mapping for node " + node.name + " already exists, skipping")
    } else {
      println("Proposing owner to key mapping for node " + node.name + "...")
      node.topology.owner_to_key_mappings.propose(
        member = node.id.member,
        keys = NonEmpty(Seq, sequencerAuthKey, signingKey),
        signedBy =
          Seq(namespaceKey.fingerprint, sequencerAuthKey.fingerprint, signingKey.fingerprint),
      )
    }
  }

// The temporary topology store of a node, which holds the transactions of the setup across phases. It lives in the
// memory of the node, so it is still there when the script is rerun.
def findTemporaryStore(node: SequencerReference, name: String): Option[TopologyStoreId.Temporary] =
  node.topology.stores.list().collectFirst {
    case store: TopologyStoreId.Temporary if store.name.unwrap == name => store
  }

// Only phase 2 creates it, and only if a previous run has not created it already.
def getOrCreateTemporaryStore(node: SequencerReference, name: String): TopologyStoreId.Temporary =
  findTemporaryStore(node, name)
    .getOrElse(node.topology.stores.create_temporary_topology_store(name, protocolVersion))

// The later phases expect phase 2 to have created it.
def getTemporaryStore(node: SequencerReference, name: String): TopologyStoreId.Temporary =
  findTemporaryStore(node, name)
    .getOrElse(sys.error(s"${node.name} has no temporary topology store $name, was phase 2 done?"))

// The transactions of the given kinds in the store that the node has signed, whether fully authorized or still
// proposals. Used to share them again on a rerun, instead of proposing them a second time.
def signedByNode(
    node: SequencerReference,
    store: TopologyStoreId,
    codes: Seq[TopologyMapping.Code],
): Seq[GenericSignedTopologyTransaction] =
  Seq(true, false).flatMap { proposals =>
    node.topology.transactions
      .list(
        store = store,
        proposals = proposals,
        filterMappings = codes,
        filterAuthorizedKey = Some(fingerprint(node)),
      )
      .result
      .map(_.transaction)
  }

def fingerprint(node: InstanceReference) = node.id.uid.namespace.fingerprint

// ---------------------------------------------------------------------------------------------------------------
// Each phase below ends where the docs have an operator share a file with the others, and the next phase starts
// by reading the files the others shared. The phases keep no state between them other than the files (and the
// nodes themselves), so every operator could run its part separately.
// ---------------------------------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------------------------------
// Phase 0: agree on the static synchronizer parameters
// Everybody must use the same ones. One operator defines them.
// Shares: params.proto.
// ---------------------------------------------------------------------------------------------------------------

StaticSynchronizerParameters
  .defaultsWithoutKMS(ProtocolVersion.latest)
  .writeToFile(exchangeFile("params.proto"))

// ---------------------------------------------------------------------------------------------------------------
// Phase 1: every operator initializes its nodes.
// Needs: params.proto.
// Shares: identity-<i>.bin, the onboarding transactions (namespaces and keys) of its sequencer, mediator and
// participant. They contain no secrets.
// ---------------------------------------------------------------------------------------------------------------

val staticSynchronizerParameters =
  StaticSynchronizerParameters.tryReadFromFile(exchangeFile("params.proto"))
val protocolVersion = staticSynchronizerParameters.protocolVersion

nodes.foreach { node =>
  initManually(node.sequencer)
  initManually(node.mediator)
  println(s"Waiting for ${node.participant.name} to be initialized...")
  node.participant.health.wait_for_initialized()

  writeTransactions(
    s"identity-${node.index}.bin",
    node.sequencer.topology.transactions.generate_onboarding_transactions(protocolVersion) ++
      node.mediator.topology.transactions.generate_onboarding_transactions(protocolVersion) ++
      node.participant.topology.transactions.generate_onboarding_transactions(protocolVersion),
    protocolVersion,
  )
}

// ---------------------------------------------------------------------------------------------------------------
// Phase 2: every operator signs the decentralized namespace that will own the synchronizer.
// Needs: the identity files of all nodes. They tell it the ids of all the sequencers, mediators and participants,
// which it needs to build the genesis topology, grant permissions and set traffic.
// Shares: dns-<i>.bin, its signed proposal, made with the key of its sequencer, which is one of the owners.
// ---------------------------------------------------------------------------------------------------------------

val identityTransactions = nodes.flatMap(node => readTransactions(s"identity-${node.index}.bin"))
val identityMembers: Seq[Member] =
  identityTransactions.flatMap(_.selectMapping[OwnerToKeyMapping].toList).map(_.mapping.member).distinct
val sequencerIds: Seq[SequencerId] = identityMembers.collect { case id: SequencerId => id }
val mediatorIds: Seq[MediatorId] = identityMembers.collect { case id: MediatorId => id }
val participantIds: Seq[ParticipantId] = identityMembers.collect { case id: ParticipantId => id }
require(
  sequencerIds.size == numNodes && mediatorIds.size == numNodes && participantIds.size == numNodes,
  s"Expected $numNodes sequencers, mediators and participants, found $identityMembers",
)

// The synchronizer is owned by the sequencers.
val ownerNamespaces = sequencerIds.map(_.uid.namespace).toSet
val physicalSynchronizerId = PhysicalSynchronizerId(
  SynchronizerId(
    UniqueIdentifier.tryCreate(
      synchronizerName,
      DecentralizedNamespaceDefinition.computeNamespace(ownerNamespaces),
    )
  ),
  staticSynchronizerParameters.serial,
  staticSynchronizerParameters.protocolVersion,
)
val synchronizerId = physicalSynchronizerId.logical
val setupStoreName = s"$synchronizerName-setup"

val alreadyBootstrapped =
  nodes.forall(node => node.sequencer.health.initialized() && node.mediator.health.initialized())

if (alreadyBootstrapped) {
  println("Synchronizer is already bootstrapped, skipping phases 2 to 4")
} else {
  nodes.foreach { node =>
    println(s"Signing the decentralized namespace with ${node.sequencer.name}...")
    // A temporary topology store that holds all identities.
    val tempStore = getOrCreateTemporaryStore(node.sequencer, setupStoreName)
    node.sequencer.topology.transactions.load(identityTransactions, tempStore, ForceFlag.AlienMember)
    val dnsCode = Seq(DecentralizedNamespaceDefinition.code)
    val proposed = signedByNode(node.sequencer, tempStore, dnsCode)
    val decentralizedNamespace =
      if (proposed.nonEmpty) proposed
      else
        Seq(
          node.sequencer.topology.decentralized_namespaces.propose_new(
            ownerNamespaces,
            PositiveInt.tryCreate(3),
            store = tempStore,
          )
        )
    writeTransactions(s"dns-${node.index}.bin", decentralizedNamespace, protocolVersion)
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Phase 3: every operator signs the genesis topology of the synchronizer.
// Needs: the identity files and the dns files of all nodes. Loading the proposals of all owners makes the
// decentralized namespace fully authorized, so that it can authorize the genesis topology transactions.
// Shares: genesis-<i>.bin, its signed proposals, made with the key of its sequencer.
// ---------------------------------------------------------------------------------------------------------------

if (!alreadyBootstrapped) {
  nodes.foreach { node =>
    println(s"Signing the genesis topology with ${node.sequencer.name}...")
    val tempStore = getTemporaryStore(node.sequencer, setupStoreName)
    node.sequencer.topology.transactions.load(identityTransactions, tempStore, ForceFlag.AlienMember)
    nodes.foreach { signer =>
      node.sequencer.topology.transactions.load(
        readTransactions(s"dns-${signer.index}.bin"),
        tempStore,
        ForceFlag.AlienMember,
      )
    }
    val genesisCodes = Seq(
      SynchronizerParametersState.code,
      MediatorSynchronizerState.code,
      SequencerSynchronizerState.code,
    )
    val proposed = signedByNode(node.sequencer, tempStore, genesisCodes)
    // This also proposes the transactions in the store, so on a rerun share what is already there.
    val genesisTxs =
      if (proposed.nonEmpty) proposed
      else
        node.sequencer.topology.synchronizer_bootstrap.generate_genesis_topology(
          physicalSynchronizerId,
          sequencerIds,
          sequencerIds,
          mediatorIds,
          store = tempStore,
          mediatorThreshold = PositiveInt.tryCreate(2),
        )
    writeTransactions(s"genesis-${node.index}.bin", genesisTxs, protocolVersion)
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Phase 4: every operator merges the signatures of all owners into the initial topology state of the synchronizer
// by loading them into a temporary topology store, and initializes its sequencer and mediator from it.
// Needs: the identity, dns and genesis files of all nodes.
// Nothing is shared: this is the last step that uses files.
// ---------------------------------------------------------------------------------------------------------------

if (!alreadyBootstrapped) {
  nodes.foreach { node =>
    val tempStore = getTemporaryStore(node.sequencer, setupStoreName)
    node.sequencer.topology.transactions.load(identityTransactions, tempStore, ForceFlag.AlienMember)
    // The namespace needs the signatures of enough owners to be authorized before it can authorize the genesis
    // transactions, so load all the namespace proposals first.
    (nodes.map(signer => s"dns-${signer.index}.bin") ++ nodes.map(signer => s"genesis-${signer.index}.bin"))
      .foreach { file =>
        node.sequencer.topology.transactions.load(
          readTransactions(file),
          tempStore,
          ForceFlag.AlienMember,
        )
      }

    if (node.sequencer.health.initialized()) {
      println(s"${node.sequencer.name} is already initialized, skipping")
    } else {
      println(s"Initializing ${node.sequencer.name}...")
      val initialSnapshot =
        node.sequencer.topology.transactions.export_topology_snapshotV2(store = tempStore)
      node.sequencer.setup
        .assign_from_genesis_stateV2(initialSnapshot, staticSynchronizerParameters)
        .discard
    }

    if (node.mediator.health.initialized()) {
      println(s"${node.mediator.name} is already initialized, skipping")
    } else {
      println(s"Initializing ${node.mediator.name}...")
      node.mediator.setup.assign(
        physicalSynchronizerId,
        // The mediator connects to the sequencer of its own namespace, which it knows as ownsequencer.
        SequencerConnections.tryMany(
          Seq(ownsequencer.sequencerConnection.withAlias(SequencerAlias.tryCreate(ownsequencer.name))),
          PositiveInt.one,
          NonNegativeInt.zero,
          SubmissionRequestAmplification.NoAmplification,
          SequencerConnectionPoolDelays.default,
          SubscriptionLivenessLimits.default,
        ),
        SequencerConnectionValidation.Disabled,
      )
    }

    utils.retry_until_true(node.sequencer.health.initialized() && node.mediator.health.initialized())
    node.sequencer.topology.stores.drop_temporary_topology_store(tempStore)
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Phase 5: configure the synchronizer
// Nothing is exchanged through files any more: every operator submits its proposals through its own sequencer,
// and they take effect once enough owners have signed.
// ---------------------------------------------------------------------------------------------------------------

// Every operator confirms that the other operators have initialized their sequencers too, by waiting until its own
// sequencer sees all of them in the ordering topology.
nodes.foreach { node =>
  println(s"Waiting for ${node.sequencer.name} to see all sequencers...")
  utils.retry_until_true(
    sequencerIds.forall(node.sequencer.bft.get_ordering_topology().sequencerIds.contains)
  )
}

// Runs a step that requires a proposal from every signing sequencer. Each of them proposes it unless its own view
// shows that it has already taken effect, or that it has already proposed it. Then every operator waits until the
// step has taken effect in the view of its own sequencer.
def proposeBySigningSequencers(description: String)(
    isDone: SequencerReference => Boolean,
    hasProposed: SequencerReference => Boolean,
    propose: SequencerReference => Unit,
): Unit = {
  println(s"$description...")
  signingNodes.map(_.sequencer).foreach { sequencer =>
    if (isDone(sequencer)) println(s"${sequencer.name} already sees this as done, skipping")
    else if (hasProposed(sequencer)) println(s"${sequencer.name} has already proposed, skipping")
    else propose(sequencer)
  }
  nodes.foreach(node => utils.retry_until_true(isDone(node.sequencer)))
}

def hasSequencerThreshold2(node: InstanceReference, proposals: Boolean) =
  node.topology.sequencers
    .list(store = Some(TopologyStoreId.Synchronizer(synchronizerId)), proposals = proposals)
    .exists(r =>
      r.item.threshold == PositiveInt.tryCreate(2) &&
        (!proposals || r.context.signedBy.contains(fingerprint(node)))
    )

def hasProposedSynchronizerParameters(
    node: InstanceReference,
    predicate: DynamicSynchronizerParameters => Boolean,
) =
  node.topology.synchronizer_parameters
    .list(store = TopologyStoreId.Synchronizer(synchronizerId), proposals = true)
    .exists(r => predicate(r.item) && r.context.signedBy.contains(fingerprint(node)))

proposeBySigningSequencers("Setting sequencer threshold to 2")(
  isDone = hasSequencerThreshold2(_, proposals = false),
  hasProposed = hasSequencerThreshold2(_, proposals = true),
  propose = node =>
    node.topology.sequencers.propose(
      synchronizerId,
      threshold = PositiveInt.tryCreate(2),
      active = sequencerIds,
      signedBy = Some(fingerprint(node)),
      synchronize = None,
    ),
)

proposeBySigningSequencers("Restricting onboarding")(
  isDone = _.topology.synchronizer_parameters
    .get_dynamic_synchronizer_parameters(synchronizerId)
    .onboardingRestriction == OnboardingRestriction.RestrictedOpen,
  hasProposed = hasProposedSynchronizerParameters(
    _,
    _.onboardingRestriction == OnboardingRestriction.RestrictedOpen,
  ),
  propose = node =>
    node.topology.synchronizer_parameters.propose_update(
      synchronizerId,
      _.update(onboardingRestriction = OnboardingRestriction.RestrictedOpen),
      signedBy = Some(fingerprint(node)),
    ),
)

proposeBySigningSequencers("Turning on traffic management")(
  isDone = _.topology.synchronizer_parameters
    .get_dynamic_synchronizer_parameters(synchronizerId)
    .trafficControl
    .isDefined,
  hasProposed = hasProposedSynchronizerParameters(_, _.trafficControl.isDefined),
  propose = node =>
    node.topology.synchronizer_parameters.propose_update(
      synchronizerId,
      _.update(trafficControl =
        Some(
          TrafficControlParameters(
            maxBaseTrafficAmount = NonNegativeLong.zero,
            maxBaseTrafficAccumulationDuration = PositiveFiniteDuration.ofMinutes(
              10
            ), // Doesn't really matter, since the base rate is zero.
            readVsWriteScalingFactor = PositiveInt.tryCreate(4),
            setBalanceRequestSubmissionWindowSize = PositiveFiniteDuration.ofMinutes(2),
            enforceRateLimiting = true,
            baseEventCost = NonNegativeLong.zero,
            freeConfirmationResponses = true,
          )
        )
      ),
      signedBy = Some(fingerprint(node)),
    ),
)

// Onboarding is restricted, so every participant needs a permission from the synchronizer owners before it can connect.
participantIds.foreach { participantId =>
  def hasPermission(node: InstanceReference, proposals: Boolean) =
    node.topology.participant_synchronizer_permissions
      .list(
        store = TopologyStoreId.Synchronizer(synchronizerId),
        proposals = proposals,
        filterUid = participantId.filterString,
      )
      .exists(r => !proposals || r.context.signedBy.contains(fingerprint(node)))

  proposeBySigningSequencers(s"Granting synchronizer permission to ${participantId.uid.identifier}")(
    isDone = hasPermission(_, proposals = false),
    hasProposed = hasPermission(_, proposals = true),
    propose = node =>
      node.topology.participant_synchronizer_permissions.propose(
        synchronizerId,
        participantId,
        ParticipantPermission.Submission,
        synchronize = None,
      ),
  )
}

val maxTraffic = NonNegativeLong.tryCreate(Long.MaxValue)

// Base traffic is zero, so every participant and mediator needs purchased traffic to be able to submit anything.
// We always use serial 1: on reruns the sequencers ignore the update as outdated, which is fine
// since the balance is already max.
// The update is delivered as soon as a threshold of sequencers submitted it, and further submissions
// of it are then refused, so only the 2 sequencers required by the threshold set above submit it.
// Resubmitting within the same submission window would be refused for the same reason, so we skip
// members that already have a traffic balance. The sequencers only report the traffic state of
// members that are known in the topology, so for participants this only works once they have connected.
// TODO(canton-network/splice-multi-sync#87): Once we integrate the dSync operator app, it will initialize the traffic
// balances for mediators, so those can be skipped here.
val trafficMembers: Seq[Member] = participantIds ++ mediatorIds
trafficNodes.foreach { node =>
  trafficMembers.foreach { member =>
    val name = member.uid.identifier.unwrap
    val hasTrafficBalance = node.sequencer.traffic_control
      .traffic_state_of_members(Seq(member))
      .trafficStates
      .get(member)
      .exists(_.serial.isDefined)
    if (hasTrafficBalance) {
      println(s"$name already has a traffic balance in the view of ${node.sequencer.name}, skipping")
    } else {
      println(s"${node.sequencer.name} sets the traffic balance of $name to max...")
      node.sequencer.traffic_control.set_traffic_balance(member, PositiveInt.one, maxTraffic)
    }
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Phase 6: every operator connects its participant to the sequencer in its own namespace.
// ---------------------------------------------------------------------------------------------------------------

val synchronizerAlias = SynchronizerAlias.tryCreate(synchronizerName)
nodes.foreach { node =>
  if (node.participant.synchronizers.is_connected(synchronizerId)) {
    println(s"${node.participant.name} is already connected to the synchronizer, skipping")
  } else {
    println(s"Connecting ${node.participant.name} to the synchronizer...")
    // synchronize = None: the default waits for all console nodes to be idle, including ownsequencer, whose admin API isn't reachable from this console.
    node.participant.synchronizers.connect_local(ownsequencer, synchronizerAlias, synchronize = None)
    utils.retry_until_true(node.participant.synchronizers.is_connected(synchronizerId))
  }
}

println("dSync bootstrapped and ready. Running ping test...")
nodes(0).participant.health.ping(nodes(1).participant.id, 10.seconds, synchronizerId)
