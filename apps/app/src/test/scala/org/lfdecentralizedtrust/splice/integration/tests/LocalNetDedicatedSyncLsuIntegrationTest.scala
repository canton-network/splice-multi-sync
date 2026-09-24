package org.lfdecentralizedtrust.splice.integration.tests

import better.files.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Synchronizer

import java.time.Duration
import scala.concurrent.duration.*

/** Verifies that the sync operator upgrades its dedicated synchronizer on its own schedule, and
  * that members keep both what they purchased and what they have already spent.
  *
  * This spins up the docker-compose localnet with the sync operator enabled (-O)
  */
class LocalNetDedicatedSyncLsuIntegrationTest extends LocalNetDedicatedSyncIntegrationTestBase {

  // Bind-mounted into the sync operator container, see its app.conf.
  private val scheduleDir = File("cluster/compose/localnet/conf/splice/sync-operator")

  // The serial the successor is configured with, see the same app.conf.
  private val successorSerial = NonNegativeInt.tryCreate(2)

  // Each step needs a poll at the default interval, so the window is generous.
  private val freezeIn = Duration.ofMinutes(1)
  private val upgradeIn = Duration.ofMinutes(6)

  private val upgradeTimeout = 10.minutes

  private def scheduleFiles =
    Seq("lsu-topology-freeze-time", "lsu-upgrade-time").map(scheduleDir / _)

  private def clearSchedule(): Unit =
    scheduleFiles.foreach(f => if (f.exists) f.delete())

  // A file left behind by an earlier run would schedule an upgrade with a past time.
  override def beforeAll(): Unit = {
    super.beforeAll()
    clearSchedule()
  }

  override def afterAll(): Unit = {
    clearSchedule()
    super.afterAll()
  }

  "the sync operator upgrades its dedicated synchronizer and preserves traffic" in { implicit env =>
    withLocalNet(Seq("-O")) { implicit env =>
      val participant = participantClient("app-provider")
      val appSynchronizerId = synchronizerId(participant, "app-synchronizer")

      def trafficState() = participant.traffic_control.traffic_state(appSynchronizerId)

      def connectedPsid(): PhysicalSynchronizerId =
        participant.synchronizers
          .list_connected()
          .find(_.synchronizerAlias.unwrap == "app-synchronizer")
          .getOrElse(fail("app-provider is not connected to app-synchronizer"))
          .physicalSynchronizerId

      val registration = registerSynchronizer(appSynchronizerId, operatorParty(participant))
      val buyer = vc("providerValidatorClient").copy(token = Some(token)).getValidatorPartyId()
      val predecessorPsid = connectedPsid()

      actAndCheck(automationTimeout)(
        "the participant buys traffic for the dedicated synchronizer",
        buyTraffic(participant, buyer, appSynchronizerId, registration),
      )(
        "the operator grants it on the predecessor's sequencer",
        _ => trafficState().extraTrafficPurchased.value shouldBe purchasedTraffic,
      )

      clue("some of it is spent, so there is consumption to carry across the upgrade") {
        participant.health.ping(participant.id, synchronizerId = Some(appSynchronizerId))
        eventually(automationTimeout) {
          trafficState().extraTrafficConsumed.value should be > 0L
        }
      }

      val purchasedBefore = trafficState().extraTrafficPurchased.value
      val consumedBefore = trafficState().extraTrafficConsumed.value

      val upgradeTime = actAndCheck(automationTimeout)(
        "the operator schedules the upgrade", {
          val now = env.environment.clock.now
          val upgradeTime = now.plus(upgradeIn)
          (scheduleDir / "lsu-topology-freeze-time")
            .overwrite(now.plus(freezeIn).toInstant.toString)
          (scheduleDir / "lsu-upgrade-time").overwrite(upgradeTime.toInstant.toString)
          upgradeTime
        },
      )(
        "the announcement is published on the predecessor once the freeze time is reached",
        _ =>
          participant.topology.lsu.announcement
            .list(store = Some(Synchronizer(appSynchronizerId)))
            .map(_.item.successorSynchronizerId.serial) should contain(successorSerial),
      )._1

      clue("the successor's sequencer and mediator are initialized from the predecessor") {
        eventually(upgradeTimeout) {
          val successors = participant.topology.lsu.sequencer_successors
            .list(store = Some(Synchronizer(appSynchronizerId)))
          // Published once the successor is initialized, carrying the predecessor's sequencer id,
          // which is what lets the participant follow.
          successors.map(_.item.successorPsid.serial) should contain(successorSerial)
        }
      }

      clue(s"the participant follows the upgrade onto the successor at $upgradeTime") {
        eventually(upgradeTimeout) {
          val psid = connectedPsid()
          psid.serial shouldBe successorSerial
          psid.logical shouldBe predecessorPsid.logical
        }
      }

      clue("both halves of the traffic state survived the upgrade") {
        eventually(upgradeTimeout) {
          val state = trafficState()
          state.extraTrafficPurchased.value shouldBe purchasedBefore
          // Without the transfer this resets to zero and members get back what they spent.
          state.extraTrafficConsumed.value should be >= consumedBefore
        }
      }

      actAndCheck(upgradeTimeout)(
        "a further purchase lands after the upgrade",
        buyTraffic(participant, buyer, appSynchronizerId, registration),
      )(
        "the operator grants it on the successor's sequencer",
        _ => trafficState().extraTrafficPurchased.value shouldBe purchasedBefore + purchasedTraffic,
      )

      clue("and the participant can still transact on the upgraded synchronizer") {
        participant.health.ping(participant.id, synchronizerId = Some(appSynchronizerId))
      }
    }
  }

}
