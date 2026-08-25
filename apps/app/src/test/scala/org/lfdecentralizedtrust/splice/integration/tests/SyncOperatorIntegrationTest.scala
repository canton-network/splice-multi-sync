// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.integration.tests

import com.digitalasset.canton.SynchronizerAlias
import org.lfdecentralizedtrust.splice.integration.EnvironmentDefinition
import org.lfdecentralizedtrust.splice.integration.tests.SpliceTests.IntegrationTestWithIsolatedEnvironment

class SyncOperatorIntegrationTest extends IntegrationTestWithIsolatedEnvironment {

  override def environmentDefinition: SpliceEnvironmentDefinition =
    EnvironmentDefinition
      .fromResources(
        Seq("simple-topology-1sv.conf", "sync-operator-topology.conf"),
        this.getClass.getSimpleName,
      )
      .withStandardSetup
      .withManualStart

  "sync operator app" should {

    "start and restart cleanly" in { implicit env =>
      initDsoWithSv1Only()
      splitwellValidatorBackend.startSync()

      // startSync fails the test unless the app reports itself active within the timeout.
      syncOperatorBackend.startSync()

      clue("it takes its synchronizer id from the sequencer it is configured with") {
        val served = splitwellValidatorBackend.participantClientWithAdminToken.synchronizers
          .id_of(SynchronizerAlias.tryCreate("splitwell"))
          .logical
        syncOperatorBackend.appState.store.key.synchronizerId shouldBe served
      }

      syncOperatorBackend.stop()
      syncOperatorBackend.is_running shouldBe false

      syncOperatorBackend.startSync()
    }
  }
}
