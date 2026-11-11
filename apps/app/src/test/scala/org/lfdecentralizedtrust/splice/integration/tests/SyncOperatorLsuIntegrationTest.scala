// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import better.files.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.{
  FullClientConfig,
  NonNegativeDuration,
  NonNegativeFiniteDuration,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  ParticipantSynchronizerPermission,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.lfdecentralizedtrust.splice.automation.Trigger
import org.lfdecentralizedtrust.splice.config.ConfigTransforms
import org.lfdecentralizedtrust.splice.console.ParticipantClientReference
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.{
  IntegrationTest,
  SpliceTestConsoleEnvironment,
}
import org.lfdecentralizedtrust.splice.lsu.{LsuRollForwardTimestamp, LsuTransferTrafficTrigger}
import org.lfdecentralizedtrust.splice.syncoperator.automation.{
  DedicatedLsuTrigger,
  UnlimitedTrafficTrigger,
}
import org.lfdecentralizedtrust.splice.syncoperator.automation.UnlimitedTrafficTrigger.UnlimitedTraffic
import org.lfdecentralizedtrust.splice.syncoperator.config.{
  SyncOperatorLsuConfig,
  SyncOperatorMediatorConfig,
  SyncOperatorSequencerConfig,
  SyncOperatorSynchronizerNodeConfig,
}
import org.lfdecentralizedtrust.splice.util.{StandaloneCanton, TriggerTestUtil}

import java.time.Duration
import scala.concurrent.duration.*

/** Upgrades the synchronizer the operator runs, on a schedule the operator sets itself, and checks
  * that members keep unlimited traffic, including a participant admitted before the upgrade that
  * joins only after it.
  *
  * The successor's sequencer and mediator are started only for this test, uninitialized, so the
  * operator's LSU trigger initializes them from the synchronizer it is upgrading.
  */
class SyncOperatorLsuIntegrationTest
    extends IntegrationTest
    with StandaloneCanton
    with TriggerTestUtil {

  override def dbsSuffix = "sync_operator_lsu"

  override def usesDbs: Seq[String] = Seq(
    s"sequencer_${dbsSuffix}_successor",
    s"sequencer_driver_${dbsSuffix}_successor",
    s"mediator_${dbsSuffix}_successor",
  ) ++ super.usesDbs

  private val splitwellAlias = SynchronizerAlias.tryCreate("splitwell")

  // With start-canton.sh -t the synchronizer is bootstrapped at serial 0 and protocol version 37,
  // see bootstrap-canton.sc.
  private val successorSerial = NonNegativeInt.one
  private val successorPv = ProtocolVersion.v37

  // Ports of the standalone successor nodes, see standalone-sync-operator-successor.conf.
  private val successorSequencerAdminPort = Port.tryCreate(27609)
  private val successorSequencerPublicPort = Port.tryCreate(27608)
  private val successorMediatorAdminPort = Port.tryCreate(27607)

  // The operator reads its schedule from files, so the test writes them once it is ready rather
  // than picking a time while the environment is still starting.
  private lazy val scheduleDir = File.newTemporaryDirectory("sync-operator-lsu")
  private lazy val freezeTimeFile = scheduleDir / "topology-freeze-time"
  private lazy val upgradeTimeFile = scheduleDir / "upgrade-time"

  override def afterAll(): Unit = {
    scheduleDir.delete(swallowIOExceptions = true)
    super.afterAll()
  }

  override def environmentDefinition: SpliceEnvironmentDefinition =
    EnvironmentDefinition
      .fromResources(
        Seq("simple-topology-1sv.conf", "sync-operator-topology.conf"),
        this.getClass.getSimpleName,
      )
      .withOnlyAliceValidatorConnectingToSplitwell
      .withStandardSetup
      .addConfigTransform((_, config) =>
        ConfigTransforms.updateAllSyncOperatorAppConfigs_ { c =>
          c.copy(
            synchronizerNodes = c.synchronizerNodes.copy(
              // The mediator is only read during an upgrade, so the shared topology leaves it out.
              current = c.synchronizerNodes.current.copy(
                mediator = Some(
                  SyncOperatorMediatorConfig(FullClientConfig(port = Port.tryCreate(5707)))
                )
              ),
              successor = Some(
                SyncOperatorSynchronizerNodeConfig(
                  sequencer = SyncOperatorSequencerConfig(
                    adminApi = FullClientConfig(port = successorSequencerAdminPort),
                    internalApi = Some(FullClientConfig(port = successorSequencerPublicPort)),
                    externalPublicApiUrl =
                      Some(s"http://localhost:${successorSequencerPublicPort.unwrap}"),
                  ),
                  mediator = Some(
                    SyncOperatorMediatorConfig(FullClientConfig(port = successorMediatorAdminPort))
                  ),
                  serial = Some(successorSerial),
                )
              ),
            ),
            lsu = Some(
              SyncOperatorLsuConfig(
                topologyFreezeTime = LsuRollForwardTimestamp.TimestampFromFile(freezeTimeFile.path),
                upgradeTime = LsuRollForwardTimestamp.TimestampFromFile(upgradeTimeFile.path),
                newPhysicalSynchronizerSerial = successorSerial,
                newPhysicalSynchronizerProtocolVersion = successorPv,
              )
            ),
            lsuDumpPath = Some((scheduleDir / "lsu-dump.json").path),
            parameters = c.parameters.copy(
              spliceCachingConfigs = c.parameters.spliceCachingConfigs.copy(
                physicalSynchronizerExpiration = NonNegativeFiniteDuration.ofSeconds(1)
              )
            ),
            // Both triggers reach the successor's nodes and log if they cannot, so they stay
            // paused until the test has started them.
            automation = c.automation
              .withPausedTrigger[DedicatedLsuTrigger]
              .withPausedTrigger[LsuTransferTrafficTrigger],
          )
        }(config)
      )

  "sync operator" should {

    "upgrade its synchronizer and keep its members' unlimited traffic" in { implicit env =>
      val alice = aliceValidatorBackend.participantClientWithAdminToken
      val bob = bobValidatorBackend.participantClientWithAdminToken
      val trafficTrigger = syncOperatorBackend.appState.automation.trigger[UnlimitedTrafficTrigger]

      // The bootstrap admits alice's participant, which joins splitwell during the test setup.
      clue("alice's participant is granted unlimited traffic") {
        eventually() {
          trafficState(sequencer, alice.id).map(_.extraTrafficLimit) shouldBe
            Some(UnlimitedTraffic)
        }
      }

      clue("alice's participant transacts, so there is consumption to carry across the upgrade") {
        enableParty(alice, "lsu-before-upgrade")
        eventually() {
          consumed(sequencer, alice.id) should be > 0L
        }
      }
      val consumedBefore = consumed(sequencer, alice.id)

      // From here on the traffic trigger only runs when the test runs it.
      trafficTrigger.pause().futureValue

      clue("bob's participant is admitted and bought for, but does not join yet") {
        admit(bob.id)
        eventually() {
          sequencer
            .listParticipantSynchronizerPermissions(synchronizerId)
            .futureValue
            .map(_.mapping.participantId) should contain(bob.id)
        }
        // The purchase is submitted, then found recorded once the reconciliation delay has passed,
        // after which nothing is left to do.
        eventually(40.seconds) {
          trafficTrigger.runOnce().futureValue shouldBe false
        }
        eventually(40.seconds) {
          trafficTrigger.runOnce().futureValue shouldBe true
        }
        trafficTrigger.runOnce().futureValue shouldBe false
      }

      val currentPsid = sequencer.getPhysicalSynchronizerId().futureValue
      currentPsid.serial.value should be < successorSerial.value

      withCanton(
        Seq(testResourcesPath / "standalone-sync-operator-successor.conf"),
        Seq.empty,
        "sync-operator-lsu-successor",
        "SYNC_OPERATOR_SUCCESSOR_SEQUENCER_DB" -> s"sequencer_${dbsSuffix}_successor",
        "SYNC_OPERATOR_SUCCESSOR_SEQUENCER_DRIVER_DB" -> s"sequencer_driver_${dbsSuffix}_successor",
        "SYNC_OPERATOR_SUCCESSOR_MEDIATOR_DB" -> s"mediator_${dbsSuffix}_successor",
      ) {
        val lsuTrigger = syncOperatorBackend.appState.automation.trigger[DedicatedLsuTrigger]
        val lsuTrafficTrigger =
          syncOperatorBackend.appState.automation.trigger[LsuTransferTrafficTrigger]

        setTriggersWithin(triggersToResumeAtStart = Seq[Trigger](lsuTrigger, lsuTrafficTrigger)) {
          val upgradeTime = actAndCheck(
            "the operator schedules the upgrade", {
              val now = CantonTimestamp.now()
              // Long enough for the successor to be stood up, short enough that the test
              // still crosses it.
              val upgradeTime = now.plus(Duration.ofMinutes(3))
              freezeTimeFile.overwrite(now.toInstant.toString)
              upgradeTimeFile.overwrite(upgradeTime.toInstant.toString)
              upgradeTime
            },
          )(
            "the announcement is published on the synchronizer being upgraded",
            _ =>
              sequencer
                .listLsuAnnouncements(currentPsid.logical)
                .futureValue
                .map(_.mapping.successorSynchronizerId.serial) should contain(successorSerial),
          )._1

          val successorNode = syncOperatorBackend.appState.synchronizerNodes.successor.value
          val successor = successorNode.sequencerAdminConnection

          clue(s"the successor's nodes are initialized from the predecessor before $upgradeTime") {
            eventuallySucceeds(5.minutes) {
              val successorPsid = successor.getPhysicalSynchronizerId().futureValue
              successorPsid.logical shouldBe currentPsid.logical
              successorPsid.serial shouldBe successorSerial
              successorPsid.protocolVersion shouldBe successorPv
            }
            eventually() {
              successorNode.mediatorAdminConnection.isNodeInitialized().futureValue shouldBe true
            }
          }

          clue("alice's traffic state is carried onto the successor") {
            eventually(5.minutes) {
              val state = trafficState(successor, alice.id).value
              state.extraTrafficLimit shouldBe UnlimitedTraffic
              // Without the transfer this resets to zero and members get back what they spent.
              state.extraTrafficConsumed.value should be >= consumedBefore
            }
          }

          clue(s"alice's participant follows the upgrade onto the successor at $upgradeTime") {
            eventually(10.minutes) {
              val psid = alice.synchronizers
                .list_connected()
                .find(_.synchronizerAlias == splitwellAlias)
                .value
                .physicalSynchronizerId
              psid.logical shouldBe currentPsid.logical
              psid.serial shouldBe successorSerial
            }
          }

          // The upgrade only carries over traffic for members that have joined, so the purchase
          // for bob stayed behind on the predecessor.
          clue("the traffic trigger buys for bob again once it has switched to the successor") {
            eventually(2.minutes) {
              trafficTrigger.runOnce().futureValue shouldBe true
            }
            trafficTrigger.runOnce().futureValue shouldBe false
          }

          clue("bob's participant joins the upgraded synchronizer with unlimited traffic") {
            bob.synchronizers.connect(
              splitwellAlias,
              s"http://localhost:${successorSequencerPublicPort.unwrap}",
              synchronize = None,
            )
            // The trigger is paused, so only the purchase made before bob joined can grant this.
            eventually() {
              trafficState(successor, bob.id).map(_.extraTrafficLimit) shouldBe
                Some(UnlimitedTraffic)
            }
          }

          trafficTrigger.resume()

          // The participant admin API in tests does not allow pings, so the participants submit
          // topology transactions instead.
          clue("both participants transact on the upgraded synchronizer") {
            enableParty(alice, "lsu-after-upgrade-alice")
            enableParty(bob, "lsu-after-upgrade-bob")
          }
        }

        clue("stop apps manually to prevent errors from the synchronizer being force stopped") {
          syncOperatorBackend.stop()
          alice.synchronizers.disconnect(splitwellAlias)
          bob.synchronizers.disconnect(splitwellAlias)
        }
      }
    }
  }

  private def sequencer(implicit env: SpliceTestConsoleEnvironment) =
    syncOperatorBackend.appState.sequencerAdminConnection

  private def synchronizerId(implicit env: SpliceTestConsoleEnvironment) =
    syncOperatorBackend.appState.synchronizerId

  private def trafficState(connection: SequencerAdminConnection, member: Member) =
    connection.lookupSequencerTrafficControlState(member).futureValue

  private def consumed(connection: SequencerAdminConnection, member: Member): Long =
    trafficState(connection, member).fold(0L)(_.extraTrafficConsumed.value)

  // The splitwell sequencer owns the synchronizer, so it can admit participants on its own.
  private def admit(participantId: ParticipantId)(implicit env: SpliceTestConsoleEnvironment) =
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

  private def enableParty(participant: ParticipantClientReference, name: String) =
    participant.parties.enable(
      name,
      synchronizer = Some(splitwellAlias),
      synchronizeParticipants = Seq(participant),
      synchronize = Some(NonNegativeDuration.ofSeconds(60)),
    )
}
