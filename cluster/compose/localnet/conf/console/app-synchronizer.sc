import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.data.SubmissionRequestAmplification
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.daml.lf.data.Ref

// With four BFT nodes (-B), the app-synchronizer is owned by the four sequencers together. Node 5
// is left out, for tests that onboard and remove a node.
val bft = sys.env.get("APP_SYNCHRONIZER_BFT_PROFILE").contains("on")
val nodeSuffixes = if (bft) Seq("", "-2", "-3", "-4") else Seq("")
val appSequencers = nodeSuffixes.map(suffix =>
  sequencers.all
    .find(_.name == s"app-sequencer$suffix")
    .getOrElse(sys.error(s"app-sequencer$suffix is not configured"))
)
val appMediators = nodeSuffixes.map(suffix =>
  mediators.all
    .find(_.name == s"app-mediator$suffix")
    .getOrElse(sys.error(s"app-mediator$suffix is not configured"))
)

// Thresholds as in Splice's Thresholds.scala: traffic grants and verdicts need faults + 1 nodes to
// agree, and topology changes a majority of the owners.
val faults = (appSequencers.size - 1) / 3
val sequencerThreshold = PositiveInt.tryCreate(faults + 1)
val mediatorThreshold = PositiveInt.tryCreate(faults + 1)
val ownerThreshold = PositiveInt.tryCreate(math.ceil((appSequencers.size + faults + 1) / 2.0).toInt)
// The owners that sign each topology change below.
val signers = appSequencers.take(ownerThreshold.value)

utils.retry_until_true {
  (appSequencers ++ appMediators).forall(_.health.is_running())
}

val appSynchronizerId = bootstrap.synchronizer(
  synchronizerName = "app-synchronizer",
  sequencers = appSequencers,
  // Each mediator connects only to its own node's sequencer, as SV mediators do.
  mediatorsToSequencers = appMediators
    .zip(appSequencers)
    .map { case (mediator, sequencer) =>
      mediator -> (Seq(sequencer), PositiveInt.one, NonNegativeInt.zero)
    }
    .toMap,
  synchronizerOwners = appSequencers,
  synchronizerThreshold = ownerThreshold,
  staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
  mediatorRequestAmplification = SubmissionRequestAmplification.NoAmplification,
  mediatorThreshold = mediatorThreshold,
)

// Raise the sequencer threshold, which genesis leaves at 1.
if (sequencerThreshold != PositiveInt.one) {
  signers.foreach(owner =>
    owner.topology.sequencers.propose(
      appSynchronizerId.logical,
      threshold = sequencerThreshold,
      active = appSequencers.map(_.id),
      signedBy = Some(owner.id.uid.namespace.fingerprint),
    )
  )

  utils.retry_until_true {
    `app-sequencer`.topology.sequencers
      .list(store = Some(TopologyStoreId.Synchronizer(appSynchronizerId.logical)))
      .exists(_.item.threshold == sequencerThreshold)
  }
}

if (bft) {
  // Participants read from and submit to several sequencers, with the thresholds a validator uses
  // on the global synchronizer.
  val connections = appSequencers.map(sequencer =>
    sequencer.sequencerConnection.withAlias(SequencerAlias.tryCreate(sequencer.name))
  )
  Seq(`app-provider`, `app-user`).foreach(
    _.synchronizers.connect_bft(
      connections,
      "app-synchronizer",
      sequencerTrustThreshold = sequencerThreshold,
      sequencerLivenessMargin = NonNegativeInt.tryCreate(faults),
      submissionRequestAmplification = SubmissionRequestAmplification(
        sequencerThreshold,
        com.digitalasset.canton.config.NonNegativeFiniteDuration.ofSeconds(10),
      ),
    )
  )
} else {
  `app-provider`.synchronizers.connect_local(`app-sequencer`, "app-synchronizer")
  `app-user`.synchronizers.connect_local(`app-sequencer`, "app-synchronizer")
}

utils.retry_until_true {
  `app-provider`.synchronizers.active("app-synchronizer") &&
    `app-user`.synchronizers.active("app-synchronizer")
}

// Enable the multi-synchronizer topology feature flag on every synchronizer each
// participant is connected to
val multiSyncParticipants = Seq(`app-provider`, `app-user`)

// Wait until the participants are also connected to the global synchronizer, otherwise
// we would only enable the flag on the app-synchronizer.
utils.retry_until_true {
  multiSyncParticipants.forall(
    _.synchronizers.list_connected().exists(_.synchronizerId != appSynchronizerId.logical)
  )
}

val multiSyncFeatureFlag =
  SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag.EnableMultiSynchronizer
multiSyncParticipants.foreach { participant =>
  participant.synchronizers.list_connected().map(_.synchronizerId).distinct.foreach {
    synchronizerId =>
      val existingFlags = participant.topology.synchronizer_trust_certificates
        .list(
          store = Some(TopologyStoreId.Synchronizer(synchronizerId)),
          filterUid = participant.id.filterString,
        )
        .map(_.item.featureFlags)
        .flatten
        .distinct
      if (!existingFlags.contains(multiSyncFeatureFlag)) {
        participant.topology.synchronizer_trust_certificates
          .propose(
            participant.id,
            synchronizerId,
            featureFlags = existingFlags :+ multiSyncFeatureFlag,
          )
      }
  }
}

// Ensure the flag became effective on all synchronizers before the console exits.
utils.retry_until_true {
  multiSyncParticipants.forall { participant =>
    participant.synchronizers.list_connected().map(_.synchronizerId).distinct.forall {
      synchronizerId =>
        participant.topology.synchronizer_trust_certificates
          .list(
            store = Some(TopologyStoreId.Synchronizer(synchronizerId)),
            filterUid = participant.id.filterString,
          )
          .exists(_.item.featureFlags.contains(multiSyncFeatureFlag))
    }
  }
}

// With the sync operator serving it (-O), the app-synchronizer runs traffic control with a zero
// base rate, so members transact only against traffic they buy. Without it nothing is changed.
if (sys.env.get("SYNC_OPERATOR_PROFILE").contains("on")) {
  signers.foreach(owner =>
    owner.topology.synchronizer_parameters.propose_update(
      appSynchronizerId.logical,
      // The console TrafficControlParameters has no defaults, so every field is given.
      _.update(trafficControl =
        Some(
          TrafficControlParameters(
            maxBaseTrafficAmount = NonNegativeLong.zero,
            readVsWriteScalingFactor = PositiveInt.tryCreate(200),
            maxBaseTrafficAccumulationDuration = PositiveFiniteDuration.ofMinutes(10),
            setBalanceRequestSubmissionWindowSize = PositiveFiniteDuration.ofMinutes(5),
            enforceRateLimiting = true,
            baseEventCost = NonNegativeLong.zero,
            freeConfirmationResponses = false,
          )
        )
      ),
      signedBy = Some(owner.id.uid.namespace.fingerprint),
    )
  )

  // Wait for the change to become effective before the console exits.
  utils.retry_until_true {
    `app-sequencer`.topology.synchronizer_parameters
      .get_dynamic_synchronizer_parameters(appSynchronizerId.logical)
      .trafficControl
      .isDefined
  }
}

// The app-synchronizer only admits participants its owners have permissioned. The permissions go
// on before the restriction, so the participants already connected keep their access.
multiSyncParticipants.foreach { participant =>
  signers.foreach(
    _.topology.participant_synchronizer_permissions.propose(
      appSynchronizerId.logical,
      participant.id,
      ParticipantPermission.Submission,
    )
  )
}

utils.retry_until_true {
  multiSyncParticipants.forall(participant =>
    `app-sequencer`.topology.participant_synchronizer_permissions
      .find(appSynchronizerId.logical, participant.id)
      .isDefined
  )
}

signers.foreach(owner =>
  owner.topology.synchronizer_parameters.propose_update(
    appSynchronizerId.logical,
    _.update(onboardingRestriction = OnboardingRestriction.RestrictedOpen),
    signedBy = Some(owner.id.uid.namespace.fingerprint),
  )
)

// Wait for the restriction to become effective before the console exits.
utils.retry_until_true {
  `app-sequencer`.topology.synchronizer_parameters
    .get_dynamic_synchronizer_parameters(appSynchronizerId.logical)
    .onboardingRestriction == OnboardingRestriction.RestrictedOpen
}

// With four BFT nodes (-B), each node belongs to its own org, which runs the node's sync operator
// against its own participant. The four orgs create the operator party together: they own its
// namespace and host it on their participants. Org 5's participant joins the global synchronizer
// too, for tests that add an org.
if (bft) {
  val orgParticipants = (nodeSuffixes :+ "-5").map(suffix =>
    participants.all
      .find(_.name == s"app-operator$suffix")
      .getOrElse(sys.error(s"app-operator$suffix is not configured"))
  )
  val orgs = orgParticipants.take(appSequencers.size)

  utils.retry_until_true {
    orgParticipants.forall(_.health.initialized())
  }

  // The org participants run without a validator, so they load the package of the operator's
  // contracts from the app-provider participant.
  val amuletDar = `app-provider`.dars
    .list(filterName = "splice-amulet")
    .filter(_.name == "splice-amulet")
    .maxBy(dar => Ref.PackageVersion.assertFromString(dar.version))
  val darDirectory = java.nio.file.Files.createTempDirectory("app-operator")
  `app-provider`.dars.download(amuletDar.mainPackageId, darDirectory.toString)
  val amuletDarPath = darDirectory.resolve(s"${amuletDar.name}-${amuletDar.version}.dar").toString

  orgParticipants.foreach { participant =>
    participant.synchronizers.connect("global", "http://canton:5008")
    participant.dars.upload(amuletDarPath)
  }

  val globalSynchronizerId = orgs.head.synchronizers.id_of("global")
  val globalStore = TopologyStoreId.Synchronizer(globalSynchronizerId)

  // The namespace takes effect once every org has proposed it.
  val operatorNamespace = orgs
    .map(
      _.topology.decentralized_namespaces
        .propose_new(
          owners = orgs.map(_.id.uid.namespace).toSet,
          threshold = ownerThreshold,
          store = globalStore,
        )
        .transaction
        .mapping
        .namespace
    )
    .head

  utils.retry_until_true {
    orgs.forall(
      _.topology.decentralized_namespaces
        .list(globalStore, filterNamespace = operatorNamespace.filterString)
        .nonEmpty
    )
  }

  val operatorParty = PartyId(UniqueIdentifier.tryCreate("sync-operator", operatorNamespace))

  // Each org signs for its own participant, and confirming for the party takes the same majority
  // of the orgs as changing it.
  orgs.foreach(
    _.topology.party_to_participant_mappings.propose(
      operatorParty,
      newParticipants = orgs.map(_.id -> ParticipantPermission.Confirmation),
      threshold = ownerThreshold,
      store = globalStore,
    )
  )

  utils.retry_until_true {
    orgs.forall(
      _.topology.party_to_participant_mappings
        .is_known(globalSynchronizerId, operatorParty, orgs.map(_.id))
    )
  }

  // Each org's sync operator reads as the party on the org's own participant.
  orgs.foreach(
    _.ledger_api.users.create(
      "sync-operator",
      primaryParty = Some(operatorParty),
      readAs = Set(operatorParty),
    )
  )
}
