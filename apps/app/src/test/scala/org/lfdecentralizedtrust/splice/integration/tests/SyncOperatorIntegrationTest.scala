// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import com.digitalasset.canton.admin.api.client.data.SynchronizerConnectionConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.OnboardingRestriction
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  ParticipantSynchronizerPermission,
  TopologyMapping,
}
import org.lfdecentralizedtrust.splice.console.ParticipantClientReference
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.IntegrationTest

class SyncOperatorIntegrationTest extends IntegrationTest {

  private val splitwellAlias = SynchronizerAlias.tryCreate("splitwell")

  override def environmentDefinition: SpliceEnvironmentDefinition =
    EnvironmentDefinition
      .fromResources(
        Seq("simple-topology-1sv.conf", "sync-operator-topology.conf"),
        this.getClass.getSimpleName,
      )
      .withOnlyAliceValidatorConnectingToSplitwell
      .withStandardSetup

  "sync operator" should {
    "restart cleanly" in { implicit env =>
      syncOperatorBackend.stop()
      syncOperatorBackend.startSync()
    }

    "report liveness and readiness" in { implicit env =>
      syncOperatorBackend.httpLive shouldBe true
      syncOperatorBackend.httpReady shouldBe true
    }

    "take its synchronizer id from the sequencer it is configured with" in { implicit env =>
      // Alice's participant is the one still connected to splitwell in this topology.
      val served = aliceValidatorBackend.participantClientWithAdminToken.synchronizers
        .id_of(SynchronizerAlias.tryCreate("splitwell"))
        .logical
      syncOperatorBackend.appState.store.key.synchronizerId shouldBe served
    }

    "admit participants as its onboarding restriction allows" in { implicit env =>
      // The splitwell sequencer holds the synchronizer's owner key.
      val owner = syncOperatorBackend.appState.sequencerAdminConnection
      val synchronizerId = syncOperatorBackend.appState.store.key.synchronizerId
      val alice = aliceValidatorBackend.participantClientWithAdminToken
      // Neither of these participants is on splitwell in this topology.
      val bob = bobValidatorBackend.participantClientWithAdminToken
      val splitwellParticipant = splitwellValidatorBackend.participantClientWithAdminToken

      val splitwellConnection = SynchronizerConnectionConfig.tryGrpcSingleConnection(
        splitwellAlias,
        SequencerAlias.Default,
        aliceValidatorBackend.config.domains.extra.find(_.alias == splitwellAlias).value.url,
        manualConnect = true,
      )

      // Only the onboarding handshake, which needs no traffic on this zero-base-rate synchronizer.
      def join(participant: ParticipantClientReference): Unit =
        participant.synchronizers.register_by_config(
          splitwellConnection,
          performHandshake = true,
          synchronize = None,
        )

      def joined(participantId: ParticipantId): Boolean =
        owner.listSynchronizerTrustCertificate(synchronizerId, participantId).futureValue.nonEmpty

      def permitted(participantId: ParticipantId): Boolean =
        owner
          .listAllTransactions(
            TopologyStoreId.Synchronizer(synchronizerId),
            includeMappings = Set(TopologyMapping.Code.ParticipantSynchronizerPermission),
          )
          .futureValue
          .flatMap(_.selectMapping[ParticipantSynchronizerPermission])
          .exists(_.mapping.participantId == participantId)

      def permit(participantId: ParticipantId): Unit = {
        owner
          .proposeMapping(
            TopologyStoreId.Synchronizer(synchronizerId),
            ParticipantSynchronizerPermission(
              synchronizerId,
              participantId,
              ParticipantPermission.Submission,
              limits = None,
              loginAfter = None,
            ),
            serial = PositiveInt.one,
            isProposal = false,
          )
          .futureValue
        // The sequencer checks a joining participant against its own topology store.
        eventually()(permitted(participantId) shouldBe true)
      }

      def setOnboardingRestriction(restriction: OnboardingRestriction): Unit =
        owner
          .ensureDomainParameters(synchronizerId, _.tryUpdate(onboardingRestriction = restriction))
          .futureValue

      clue("while onboarding is open, a participant joins without a permission") {
        owner
          .getSynchronizerParametersState(synchronizerId)
          .futureValue
          .mapping
          .parameters
          .onboardingRestriction shouldBe OnboardingRestriction.UnrestrictedOpen
        // Alice's validator joined splitwell when the topology started.
        joined(alice.id) shouldBe true
        permitted(alice.id) shouldBe false
      }

      clue("while onboarding is restricted, a participant without a permission is refused") {
        // The participant already on splitwell is permissioned first, so it keeps its access.
        permit(alice.id)
        setOnboardingRestriction(OnboardingRestriction.RestrictedOpen)
        assertThrowsAndLogsCommandFailures(
          join(bob),
          _.errorMessage should include("INITIAL_ONBOARDING_ERROR"),
        )
        joined(bob.id) shouldBe false
      }

      clue("while onboarding is restricted, a participant the owner permissions joins") {
        permit(splitwellParticipant.id)
        join(splitwellParticipant)
        eventually()(joined(splitwellParticipant.id) shouldBe true)
      }

      // The other tests in this CI job share this Canton and expect splitwell open to anyone.
      setOnboardingRestriction(OnboardingRestriction.UnrestrictedOpen)
    }
  }
}
