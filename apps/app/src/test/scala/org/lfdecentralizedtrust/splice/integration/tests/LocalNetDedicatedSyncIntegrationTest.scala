package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.admin.api.client.data.OnboardingRestriction
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong

/** Verifies that the sync operator serves the app-synchronizer as a dedicated synchronizer: it is
  * bootstrapped with a zero base rate and admits only permissioned participants, the DSO registers
  * it to the operator, and a member transacts on it only against traffic it has bought.
  *
  * This spins up the docker-compose localnet with the sync operator enabled (-O)
  */
class LocalNetDedicatedSyncIntegrationTest extends LocalNetDedicatedSyncIntegrationTestBase {

  "the sync operator serves the app-synchronizer as a dedicated synchronizer" in { implicit env =>
    withLocalNet(Seq("-O")) { implicit env =>
      val participant = participantClient("app-provider")
      val appSynchronizerId = synchronizerId(participant, "app-synchronizer")

      def dynamicParameters() =
        participant.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(appSynchronizerId)

      def trafficControl() = dynamicParameters().trafficControl

      def trafficState() = participant.traffic_control.traffic_state(appSynchronizerId)

      clue("the synchronizer is bootstrapped with traffic control at a zero base rate") {
        trafficControl().value.maxBaseTrafficAmount shouldBe NonNegativeLong.zero
      }

      clue("the synchronizer admits only permissioned participants, and this one is permissioned") {
        dynamicParameters().onboardingRestriction shouldBe OnboardingRestriction.RestrictedOpen
        participant.topology.participant_synchronizer_permissions
          .find(appSynchronizerId, participant.id) should not be empty
      }

      val registration = registerSynchronizer(appSynchronizerId, operatorParty(participant))

      clue("with a zero base rate the participant has no allowance of its own") {
        eventually(automationTimeout) {
          val state = trafficState()
          state.extraTrafficPurchased shouldBe NonNegativeLong.zero
          state.baseTrafficRemainder shouldBe NonNegativeLong.zero
        }
      }

      clue("with nothing bought, the participant cannot transact on the dedicated synchronizer") {
        assertThrowsAndLogsCommandFailures(
          participant.health.ping(participant.id, synchronizerId = Some(appSynchronizerId)),
          _.errorMessage should include("ping"),
        )
      }

      val buyer = vc("providerValidatorClient").copy(token = Some(token)).getValidatorPartyId()

      actAndCheck(automationTimeout)(
        "the participant buys traffic for the dedicated synchronizer on the global synchronizer",
        buyTraffic(participant, buyer, appSynchronizerId, registration),
      )(
        "the operator grants it on the dedicated synchronizer's sequencer",
        _ => trafficState().extraTrafficPurchased.value shouldBe purchasedTraffic,
      )

      clue("the purchased traffic is drawn down by transacting on the dedicated synchronizer") {
        val before = trafficState().extraTrafficConsumed.value
        participant.health.ping(participant.id, synchronizerId = Some(appSynchronizerId))
        eventually(automationTimeout) {
          trafficState().extraTrafficConsumed.value should be > before
        }
      }
    }
  }
}
