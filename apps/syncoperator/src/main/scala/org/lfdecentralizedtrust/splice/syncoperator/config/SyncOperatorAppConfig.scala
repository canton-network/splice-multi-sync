// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.config

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
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

// The sequencer this node grants traffic on.
case class SyncOperatorSequencerConfig(
    adminApi: FullClientConfig
)

case class SyncOperatorAppBackendConfig(
    override val adminApi: AdminServerConfig = AdminServerConfig(),
    override val storage: DbConfig,
    postgres: SplicePostgresConfig = SplicePostgresConfig(),
    // Ledger API user of the operator party.
    operatorUser: String,
    participantClient: ParticipantClientConfig,
    scanClient: ScanAppClientConfig,
    sequencer: SyncOperatorSequencerConfig,
    override val automation: AutomationConfig = AutomationConfig(),
    parameters: SpliceParametersConfig = SpliceParametersConfig(batching = BatchingConfig()),
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofSeconds(10),
    // Traffic control for the synchronizer this operator serves. Zero base amount so that all of
    // its traffic is paid for.
    baseTrafficAmount: NonNegativeLong = NonNegativeLong.zero,
    // As on the global synchronizer.
    readVsWriteScalingFactor: PositiveInt = PositiveInt.tryCreate(4),
    baseTrafficAccumulationDuration: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMinutes(10),
    freeConfirmationResponses: Boolean = true,
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
