// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import better.files.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{FullClientConfig, NonNegativeDuration}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.transaction.GrpcConnection
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import org.lfdecentralizedtrust.splice.console.ParticipantClientReference
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.{
  IntegrationTest,
  SpliceTestConsoleEnvironment,
}
import org.lfdecentralizedtrust.splice.lsu.{LsuRollForwardTimestamp, LsuTransferTrafficTrigger}
import org.lfdecentralizedtrust.splice.syncoperator.automation.DedicatedLsuTrigger
import org.lfdecentralizedtrust.splice.syncoperator.automation.UnlimitedTrafficTrigger.UnlimitedTraffic
import org.lfdecentralizedtrust.splice.syncoperator.config.{
  SyncOperatorAppBackendConfig,
  SyncOperatorLsuConfig,
  SyncOperatorMediatorConfig,
  SyncOperatorRollForwardLsuConfig,
  SyncOperatorSequencerConfig,
  SyncOperatorSynchronizerNodeConfig,
}
import org.lfdecentralizedtrust.splice.util.StandaloneCanton

import java.time.Duration
import scala.concurrent.duration.*

/** Rolls the operator's synchronizer forward onto new nodes after an announced upgrade that never
  * happens, and checks that members keep unlimited traffic.
  *
  * Same scenario as RollForwardLsuIntegrationTest: the operator schedules an upgrade from serial 0
  * to 1 and never initializes the successor. It then restarts with the roll-forward config and
  * initializes new nodes at serial 2. A second operator instance stands in for the restart.
  */
class SyncOperatorRollForwardLsuIntegrationTest extends IntegrationTest with StandaloneCanton {

  override def dbsSuffix = "sync_operator_roll_forward"

  override def usesDbs: Seq[String] = Seq(
    s"sequencer_${dbsSuffix}_successor",
    s"sequencer_driver_${dbsSuffix}_successor",
    s"mediator_${dbsSuffix}_successor",
  ) ++ super.usesDbs

  private val splitwellAlias = SynchronizerAlias.tryCreate("splitwell")

  // With start-canton.sh -t the synchronizer is bootstrapped at serial 0 and protocol version 37,
  // see bootstrap-canton.sc.
  private val announcedSerial = NonNegativeInt.one
  private val newSerial = NonNegativeInt.tryCreate(2)
  private val newPv = ProtocolVersion.v37

  // Ports of the standalone new nodes, see standalone-sync-operator-successor.conf.
  private val newSequencerAdminPort = Port.tryCreate(27609)
  private val newSequencerPublicPort = Port.tryCreate(27608)
  private val newMediatorAdminPort = Port.tryCreate(27607)

  private val rollForwardOperatorName = "syncOperatorRollForward"

  // The operator reads its schedule from files, so the test writes them once it is ready rather
  // than picking a time while the environment is still starting.
  private lazy val scheduleDir = File.newTemporaryDirectory("sync-operator-roll-forward-lsu")
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
      .addConfigTransform((_, config) => {
        val operatorName = InstanceName.tryCreate("syncOperator")
        val operator = config.syncOperatorApps(operatorName)
        config.copy(syncOperatorApps =
          Map(
            operatorName -> announcingOperator(operator),
            InstanceName.tryCreate(rollForwardOperatorName) -> rollForwardOperator(operator),
          )
        )
      })
      // The roll-forward operator must only start once the announcing one has stopped.
      .withManualStart

  private def newNode(serial: NonNegativeInt) =
    SyncOperatorSynchronizerNodeConfig(
      sequencer = SyncOperatorSequencerConfig(
        adminApi = FullClientConfig(port = newSequencerAdminPort),
        internalApi = Some(FullClientConfig(port = newSequencerPublicPort)),
        externalPublicApiUrl = Some(s"http://localhost:${newSequencerPublicPort.unwrap}"),
      ),
      mediator = Some(SyncOperatorMediatorConfig(FullClientConfig(port = newMediatorAdminPort))),
      serial = Some(serial),
    )

  // The mediator is only read during an upgrade, so the shared topology leaves it out.
  private def withMediator(node: SyncOperatorSynchronizerNodeConfig) =
    node.copy(mediator =
      Some(SyncOperatorMediatorConfig(FullClientConfig(port = Port.tryCreate(5707))))
    )

  /** Schedules the upgrade to the announced serial, and never initializes its successor. */
  private def announcingOperator(c: SyncOperatorAppBackendConfig) =
    c.copy(
      synchronizerNodes = c.synchronizerNodes.copy(
        current = withMediator(c.synchronizerNodes.current),
        // An upgrade can only be scheduled with a successor configured.
        successor = Some(newNode(announcedSerial)),
      ),
      lsu = Some(
        SyncOperatorLsuConfig(
          topologyFreezeTime = LsuRollForwardTimestamp.TimestampFromFile(freezeTimeFile.path),
          upgradeTime = LsuRollForwardTimestamp.TimestampFromFile(upgradeTimeFile.path),
          newPhysicalSynchronizerSerial = announcedSerial,
          newPhysicalSynchronizerProtocolVersion = newPv,
        )
      ),
      lsuDumpPath = Some((scheduleDir / "lsu-dump.json").path),
      // Paused for the whole test, so the announced upgrade never goes through.
      automation = c.automation
        .withPausedTrigger[DedicatedLsuTrigger]
        .withPausedTrigger[LsuTransferTrafficTrigger],
    )

  private def rollForwardOperator(c: SyncOperatorAppBackendConfig) =
    c.copy(
      synchronizerNodes = c.synchronizerNodes.copy(
        current = newNode(newSerial),
        legacy = Some(withMediator(c.synchronizerNodes.current)),
      ),
      rollForwardLsu = Some(
        SyncOperatorRollForwardLsuConfig(
          newPhysicalSynchronizerSerial = newSerial,
          newPhysicalSynchronizerProtocolVersion = newPv,
        )
      ),
      lsuDumpPath = Some((scheduleDir / "roll-forward-dump.json").path),
    )

  // There is no announcement for the new serial, so the new sequencer takes its sequencing bounds
  // from this override.
  private val sequencingBoundsOverride =
    "canton.sequencers.syncOperatorSuccessorSequencer.parameters.lsu-repair." +
      "lsu-sequencing-bounds-override"

  "sync operator" should {

    "roll its synchronizer forward after an announced upgrade did not happen" in { implicit env =>
      initDsoWithSv1Only()
      syncOperatorBackend.startSync()
      startAllSync(aliceValidatorBackend)
      val alice = aliceValidatorBackend.participantClientWithAdminToken

      // The bootstrap admits alice's participant, which joins splitwell when its validator starts.
      clue("alice's participant is granted unlimited traffic") {
        eventually(2.minutes) {
          trafficState(sequencer, alice.id).map(_.extraTrafficLimit) shouldBe
            Some(UnlimitedTraffic)
        }
      }

      clue("alice's participant transacts, so there is consumption to carry across") {
        enableParty(alice, "roll-forward-before")
        eventually() {
          consumed(sequencer, alice.id) should be > 0L
        }
      }
      val consumedBefore = consumed(sequencer, alice.id)

      val legacyPsid = sequencer.getPhysicalSynchronizerId().futureValue
      legacyPsid.serial.value should be < announcedSerial.value
      val newPsid = legacyPsid.copy(serial = newSerial, protocolVersion = newPv)

      val upgradeTime = actAndCheck(
        "the operator schedules an upgrade", {
          val now = CantonTimestamp.now()
          // Long enough for the announcement to be published before it.
          val upgradeTime = now.plus(Duration.ofMinutes(2))
          freezeTimeFile.overwrite(now.toInstant.toString)
          upgradeTimeFile.overwrite(upgradeTime.toInstant.toString)
          upgradeTime
        },
      )(
        "the announcement is published on the synchronizer",
        _ =>
          sequencer
            .listLsuAnnouncements(legacyPsid.logical)
            .futureValue
            .map(_.mapping.successorSynchronizerId.serial) should contain(announcedSerial),
      )._1

      withCanton(
        Seq(testResourcesPath / "standalone-sync-operator-successor.conf"),
        Seq(
          s"$sequencingBoundsOverride.lower-bound-sequencing-time-exclusive=$upgradeTime",
          s"$sequencingBoundsOverride.upgrade-time=$upgradeTime",
        ),
        "sync-operator-roll-forward-lsu",
        "SYNC_OPERATOR_SUCCESSOR_SEQUENCER_DB" -> s"sequencer_${dbsSuffix}_successor",
        "SYNC_OPERATOR_SUCCESSOR_SEQUENCER_DRIVER_DB" -> s"sequencer_driver_${dbsSuffix}_successor",
        "SYNC_OPERATOR_SUCCESSOR_MEDIATOR_DB" -> s"mediator_${dbsSuffix}_successor",
      ) {
        // The roll-forward reads the traffic state at the upgrade time, which the sequencer only
        // serves once it has sequenced past that time.
        clue(s"the upgrade time $upgradeTime passes without a successor to move to") {
          eventuallySucceeds(5.minutes) {
            sequencer.getLsuTrafficControlState(ts = None).futureValue
          }
        }

        val rollForwardBackend = syncop(rollForwardOperatorName)
        clue("the operator restarts with the roll-forward config") {
          syncOperatorBackend.stop()
          rollForwardBackend.startSync()
        }
        val newNodes = rollForwardBackend.appState.synchronizerNodes.current
        val newSequencer = newNodes.sequencerAdminConnection

        clue("the new nodes are initialized from the legacy ones at the new serial") {
          eventually() {
            newSequencer.getPhysicalSynchronizerId().futureValue shouldBe newPsid
            newNodes.mediatorAdminConnection.isNodeInitialized().futureValue shouldBe true
          }
        }

        clue("alice's traffic state is carried onto the new synchronizer") {
          eventually() {
            val state = trafficState(newSequencer, alice.id).value
            state.extraTrafficLimit shouldBe UnlimitedTraffic
            // Without the transfer this resets to zero and members get back what they spent.
            state.extraTrafficConsumed.value should be >= consumedBefore
          }
        }

        // The sync operator has no counterpart to Scan's roll-forward endpoint, so the participant
        // is migrated manually.
        clue("alice's participant moves to the new synchronizer") {
          alice.synchronizers.perform_manual_lsu(
            legacyPsid,
            newPsid,
            Some(upgradeTime),
            Map(
              newSequencer.getSequencerId.futureValue -> new GrpcConnection(
                NonEmpty.mk(Set, Endpoint("localhost", newSequencerPublicPort)),
                transportSecurity = false,
                customTrustCertificates = None,
              )
            ),
          )
          eventually(2.minutes) {
            connectedPsid(alice) shouldBe newPsid
          }
        }

        // The participant admin API in tests does not allow pings, so the participant submits
        // topology transactions instead.
        clue("alice's participant transacts on the new synchronizer") {
          enableParty(alice, "roll-forward-after")
        }

        clue("stop apps manually to prevent errors from the synchronizer being force stopped") {
          rollForwardBackend.stop()
          alice.synchronizers.disconnect(splitwellAlias)
        }
      }
    }
  }

  private def sequencer(implicit env: SpliceTestConsoleEnvironment) =
    syncOperatorBackend.appState.sequencerAdminConnection

  private def trafficState(connection: SequencerAdminConnection, member: Member) =
    connection.lookupSequencerTrafficControlState(member).futureValue

  private def consumed(connection: SequencerAdminConnection, member: Member): Long =
    trafficState(connection, member).fold(0L)(_.extraTrafficConsumed.value)

  private def connectedPsid(participant: ParticipantClientReference) =
    participant.synchronizers
      .list_connected()
      .find(_.synchronizerAlias == splitwellAlias)
      .value
      .physicalSynchronizerId

  private def enableParty(participant: ParticipantClientReference, name: String) =
    participant.parties.enable(
      name,
      synchronizer = Some(splitwellAlias),
      synchronizeParticipants = Seq(participant),
      synchronize = Some(NonNegativeDuration.ofSeconds(60)),
    )
}
