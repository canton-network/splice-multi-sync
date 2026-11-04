// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  ParticipantSynchronizerPermission,
}
import org.lfdecentralizedtrust.splice.automation.GrantUnlimitedTrafficTriggerBase.UnlimitedTraffic
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.{
  IntegrationTest,
  SpliceTestConsoleEnvironment,
}
import org.lfdecentralizedtrust.splice.syncoperator.automation.{
  ParticipantUnlimitedTrafficTrigger,
  PreJoinParticipantTrafficTrigger,
}

import scala.concurrent.duration.DurationInt

class SyncOperatorIntegrationTest extends IntegrationTest {

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
      syncOperatorBackend.appState.synchronizerId shouldBe served
    }

    "charge traffic the way the global synchronizer does" in { implicit env =>
      clue("the synchronizer runs traffic control with a zero base rate") {
        synchronizerParameters.trafficControl.map(_.maxBaseTrafficAmount.value) shouldBe Some(0L)
      }

      clue("the synchronizer charges traffic the way the global synchronizer does") {
        val participant = aliceValidatorBackend.participantClientWithAdminToken
        val globalSynchronizerId =
          participant.synchronizers.id_of(SynchronizerAlias.tryCreate("global")).logical
        eventually() {
          val global = participant.topology.synchronizer_parameters
            .get_dynamic_synchronizer_parameters(globalSynchronizerId)
            .trafficControl
            .value
          val dedicated = synchronizerParameters.trafficControl.value
          dedicated.readVsWriteScalingFactor shouldBe global.readVsWriteScalingFactor
          dedicated.freeConfirmationResponses shouldBe global.freeConfirmationResponses
        }
      }
    }

    "grant unlimited traffic to the mediator and to admitted participants only" in { implicit env =>
      val synchronizerId = syncOperatorBackend.appState.synchronizerId
      val alice = aliceValidatorBackend.participantClientWithAdminToken
      val bob = bobValidatorBackend.participantClientWithAdminToken
      val automation = syncOperatorBackend.appState.automation
      val joinedTrigger = automation.trigger[ParticipantUnlimitedTrafficTrigger]
      val preJoinTrigger = automation.trigger[PreJoinParticipantTrafficTrigger]

      clue("the mediator is granted unlimited traffic") {
        val mediator = sequencer
          .getMediatorSynchronizerState(synchronizerId, TopologySnapshot.Effective)
          .futureValue
          .mapping
          .active
          .forgetNE
          .loneElement
        eventually() {
          extraTrafficLimit(mediator) shouldBe Some(UnlimitedTraffic)
        }
      }

      joinedTrigger.pause().futureValue
      preJoinTrigger.pause().futureValue
      try {
        // Alice's validator has joined splitwell, but without traffic it cannot finish connecting.
        clue("a participant that joined without being admitted gets no traffic") {
          eventually() {
            extraTrafficLimit(alice.id) should not be empty
          }
          joinedTrigger.runOnce().futureValue shouldBe false
          preJoinTrigger.runOnce().futureValue shouldBe false
          extraTrafficLimit(alice.id) should not be Some(UnlimitedTraffic)
        }

        clue("an admitted participant is granted unlimited traffic") {
          admit(alice.id)
          eventually() {
            joinedTrigger.runOnce().futureValue shouldBe true
          }
          extraTrafficLimit(alice.id) shouldBe Some(UnlimitedTraffic)
        }

        // Only the purchase before the join can grant bob's traffic, as the trigger for joined
        // participants stays paused.
        clue("a participant admitted before it joins connects with unlimited traffic") {
          admit(bob.id)
          eventually() {
            preJoinTrigger.runOnce().futureValue shouldBe true
          }
          bob.synchronizers.connect_by_config(
            alice.synchronizers.config(SynchronizerAlias.tryCreate("splitwell")).value,
            synchronize = None,
          )
          extraTrafficLimit(bob.id) shouldBe Some(UnlimitedTraffic)
        }
      } finally {
        joinedTrigger.resume()
        preJoinTrigger.resume()
      }

      clue("the participant that joined late transacts") {
        eventuallySucceeds(timeUntilSuccess = 2.minutes) {
          bob.health.ping(alice.id, synchronizerId = Some(synchronizerId))
        }
      }
    }
  }

  private def sequencer(implicit env: SpliceTestConsoleEnvironment) =
    syncOperatorBackend.appState.sequencerAdminConnection

  private def extraTrafficLimit(member: Member)(implicit
      env: SpliceTestConsoleEnvironment
  ): Option[NonNegativeLong] =
    sequencer.lookupSequencerTrafficControlState(member).futureValue.map(_.extraTrafficLimit)

  // The splitwell sequencer owns the synchronizer, so it can admit participants on its own.
  private def admit(participantId: ParticipantId)(implicit env: SpliceTestConsoleEnvironment) = {
    val synchronizerId = syncOperatorBackend.appState.synchronizerId
    sequencer
      .proposeMapping(
        TopologyStoreId.Synchronizer(synchronizerId),
        ParticipantSynchronizerPermission(
          synchronizerId,
          participantId,
          ParticipantPermission.Submission,
          None,
          None,
        ),
        PositiveInt.one,
        isProposal = false,
      )
      .futureValue
  }

  private def synchronizerParameters(implicit env: SpliceTestConsoleEnvironment) =
    syncOperatorBackend.appState.sequencerAdminConnection
      .getSynchronizerParametersState(syncOperatorBackend.appState.synchronizerId)
      .futureValue
      .mapping
      .parameters
}
