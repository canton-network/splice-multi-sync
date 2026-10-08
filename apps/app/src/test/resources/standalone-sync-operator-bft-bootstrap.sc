import com.digitalasset.canton.admin.api.client.data.SubmissionRequestAmplification
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt

// Bootstraps the four-node synchronizer in standalone-sync-operator-bft.conf with the thresholds
// Splice sets for the global synchronizer: three of the four sequencers sign topology changes, and
// two sequencers or two mediators must agree.
val bftSequencers = sequencers.local.sortBy(_.name)
val bftMediators = mediators.local.sortBy(_.name)

val synchronizerId = bootstrap.synchronizer(
  synchronizerName = "syncOperatorBft",
  sequencers = bftSequencers,
  // Each mediator connects only to its own node's sequencer, as SV mediators do.
  mediatorsToSequencers = bftMediators
    .zip(bftSequencers)
    .map { case (mediator, sequencer) =>
      mediator -> (Seq(sequencer), PositiveInt.one, NonNegativeInt.zero)
    }
    .toMap,
  synchronizerOwners = bftSequencers,
  synchronizerThreshold = PositiveInt.tryCreate(3),
  staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.v35),
  mediatorRequestAmplification = SubmissionRequestAmplification.NoAmplification,
  mediatorThreshold = PositiveInt.tryCreate(2),
)

// Raise the sequencer threshold, which genesis leaves at 1.
bftSequencers
  .take(3)
  .foreach(owner =>
    owner.topology.sequencers.propose(
      synchronizerId.logical,
      threshold = PositiveInt.tryCreate(2),
      active = bftSequencers.map(_.id),
      signedBy = Some(owner.id.uid.namespace.fingerprint),
      synchronize = None,
    )
  )
