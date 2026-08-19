// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.config

import com.digitalasset.canton.config.*
import org.lfdecentralizedtrust.splice.config.{
  AutomationConfig,
  HttpClientConfig,
  NetworkAppClientConfig,
  ParticipantClientConfig,
  SpliceBackendConfig,
  SpliceParametersConfig,
  SplicePostgresConfig,
}
import org.lfdecentralizedtrust.splice.scan.config.ScanAppClientConfig

case class SyncOperatorSequencerConfig(
    adminApi: ClientConfig
)

// The sequencers of the synchronizer this node serves. A single entry is the expected
// configuration; the field is a sequence so that a BFT synchronizer, which needs one reconciler
// per sequencer, does not require a config migration.
case class SyncOperatorSynchronizerConfig(
    synchronizerId: String,
    sequencers: Seq[SyncOperatorSequencerConfig],
) {
  require(sequencers.nonEmpty, "at least one sequencer must be configured")
}

case class SyncOperatorAppBackendConfig(
    override val adminApi: AdminServerConfig = AdminServerConfig(),
    override val storage: DbConfig,
    postgres: SplicePostgresConfig = SplicePostgresConfig(),
    // Ledger API user of the operator party, on a participant connected to both the decentralized
    // and the dedicated synchronizer.
    operatorUser: String,
    participantClient: ParticipantClientConfig,
    scanClient: ScanAppClientConfig,
    synchronizer: SyncOperatorSynchronizerConfig,
    override val automation: AutomationConfig = AutomationConfig(),
    parameters: SpliceParametersConfig = SpliceParametersConfig(batching = BatchingConfig()),
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofSeconds(10),
    // Set to false to disable the DB-level exclusive lock that prevents two sync operator instances
    // from running concurrently against the same database.  Only disable for migration scenarios
    // where intentional overlap is required.
    instanceLockEnabled: Boolean = true,
) extends SpliceBackendConfig {
  override val nodeTypeName: String = "syncoperator"

  override def clientAdminApi: ClientConfig = adminApi.clientConfig
}

case class SyncOperatorAppClientConfig(
    adminApi: NetworkAppClientConfig
) extends HttpClientConfig {
  override def clientAdminApi: NetworkAppClientConfig = adminApi
}
