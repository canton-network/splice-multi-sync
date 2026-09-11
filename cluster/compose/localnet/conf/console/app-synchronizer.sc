val appSynchronizerId = bootstrap.synchronizer(
  synchronizerName = "app-synchronizer",
  sequencers = Seq(`app-sequencer`),
  mediators = Seq(`app-mediator`),
  synchronizerOwners = Seq(`app-sequencer`),
  synchronizerThreshold = 1,
  staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
)

`app-provider`.synchronizers.connect_local(`app-sequencer`, "app-synchronizer")
`app-user`.synchronizers.connect_local(`app-sequencer`, "app-synchronizer")

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

// The app-synchronizer is bootstrapped with traffic control at Canton's defaults.
// It can be further updated by the sync operator.
`app-sequencer`.topology.synchronizer_parameters.propose_update(
  appSynchronizerId.logical,
  // The console TrafficControlParameters has no defaults, so every field is given.
  _.update(trafficControl =
    Some(
      TrafficControlParameters(
        maxBaseTrafficAmount = NonNegativeLong.tryCreate(10 * 20 * 1024),
        readVsWriteScalingFactor = PositiveInt.tryCreate(200),
        maxBaseTrafficAccumulationDuration = PositiveFiniteDuration.ofMinutes(10),
        setBalanceRequestSubmissionWindowSize = PositiveFiniteDuration.ofMinutes(5),
        enforceRateLimiting = true,
        baseEventCost = NonNegativeLong.zero,
        freeConfirmationResponses = false,
      )
    )
  ),
  signedBy = Some(`app-sequencer`.id.uid.namespace.fingerprint),
)

// Wait for the change to become effective before the console exits.
utils.retry_until_true {
  `app-sequencer`.topology.synchronizer_parameters
    .get_dynamic_synchronizer_parameters(appSynchronizerId.logical)
    .trafficControl
    .isDefined
}
