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

    "admit a participant to its running synchronizer" in { implicit env =>
      // The splitwell sequencer holds the synchronizer's owner key.
      val owner = syncOperatorBackend.appState.sequencerAdminConnection
      val synchronizerId = syncOperatorBackend.appState.store.key.synchronizerId
      val alice = aliceValidatorBackend.participantClientWithAdminToken
      val bob = bobValidatorBackend.participantClientWithAdminToken

      def permission(participantId: ParticipantId) =
        ParticipantSynchronizerPermission(
          synchronizerId,
          participantId,
          ParticipantPermission.Submission,
          limits = None,
          loginAfter = None,
        )

      def permit(participantId: ParticipantId): Unit = {
        owner
          .proposeMapping(
            TopologyStoreId.Synchronizer(synchronizerId),
            permission(participantId),
            serial = PositiveInt.one,
            isProposal = false,
          )
          .futureValue
        // The sequencer checks a joining participant against its own topology store.
        eventually() {
          owner
            .listAllTransactions(
              TopologyStoreId.Synchronizer(synchronizerId),
              includeMappings = Set(TopologyMapping.Code.ParticipantSynchronizerPermission),
            )
            .futureValue
            .map(_.mapping) should contain(permission(participantId))
        }
      }

      def setOnboardingRestriction(restriction: OnboardingRestriction): Unit =
        owner
          .ensureDomainParameters(synchronizerId, _.tryUpdate(onboardingRestriction = restriction))
          .futureValue

      clue("the owner permissions the participant already connected, then restricts onboarding") {
        permit(alice.id)
        setOnboardingRestriction(OnboardingRestriction.RestrictedOpen)
      }

      // Bob's validator does not connect to splitwell, so his participant is given the sequencer
      // alice's validator connects to.
      bob.synchronizers.register_by_config(
        SynchronizerConnectionConfig.tryGrpcSingleConnection(
          splitwellAlias,
          SequencerAlias.Default,
          aliceValidatorBackend.config.domains.extra.find(_.alias == splitwellAlias).value.url,
          manualConnect = true,
        ),
        performHandshake = false,
        synchronize = None,
      )

      clue("a participant the owner has not permissioned is refused") {
        assertThrowsAndLogsCommandFailures(
          bob.synchronizers.reconnect(splitwellAlias, retry = false, synchronize = None),
          _.errorMessage should include("INITIAL_ONBOARDING_ERROR"),
        )
      }

      clue("once the owner permissions it, the same participant joins") {
        permit(bob.id)
        bob.synchronizers.reconnect(splitwellAlias, synchronize = None) shouldBe true
        eventually() {
          bob.synchronizers.active(splitwellAlias) shouldBe true
        }
      }

      // The other tests in this CI job share this Canton and expect splitwell open to anyone.
      bob.synchronizers.disconnect(splitwellAlias)
      setOnboardingRestriction(OnboardingRestriction.UnrestrictedOpen)
    }
  }
}
