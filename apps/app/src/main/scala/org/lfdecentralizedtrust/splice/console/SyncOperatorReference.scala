// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.console

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.{BaseInspection, Help}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.version.ProtocolVersion
import org.lfdecentralizedtrust.splice.config.NetworkAppClientConfig
import org.lfdecentralizedtrust.splice.environment.SpliceConsoleEnvironment
import org.lfdecentralizedtrust.splice.syncoperator.admin.api.client.commands.HttpSyncOperatorAdminAppClient
import org.lfdecentralizedtrust.splice.syncoperator.{SyncOperatorApp, SyncOperatorAppBootstrap}
import org.lfdecentralizedtrust.splice.syncoperator.automation.SyncOperatorAutomationService
import org.lfdecentralizedtrust.splice.syncoperator.config.{
  SyncOperatorAppBackendConfig,
  SyncOperatorAppClientConfig,
}

/** Sync operator app reference. */
abstract class SyncOperatorAppReference(
    override val spliceConsoleEnvironment: SpliceConsoleEnvironment,
    override val name: String,
) extends HttpAppReference {

  override def basePath = "/api/syncoperator"

  @Help.Summary("Schedule a logical synchronizer upgrade of this operator's synchronizer")
  def scheduleLogicalSynchronizerUpgrade(
      topologyFreezeTime: CantonTimestamp,
      upgradeTime: CantonTimestamp,
      newPhysicalSynchronizerSerial: NonNegativeInt,
      newPhysicalSynchronizerProtocolVersion: ProtocolVersion,
  ): Unit =
    consoleEnvironment.run {
      httpCommand(
        HttpSyncOperatorAdminAppClient.ScheduleLogicalSynchronizerUpgrade(
          topologyFreezeTime,
          upgradeTime,
          newPhysicalSynchronizerSerial,
          newPhysicalSynchronizerProtocolVersion,
        )
      )
    }

  @Help.Summary("Cancel the scheduled upgrade by removing its LSU announcement")
  def cancelLogicalSynchronizerUpgrade(): Unit =
    consoleEnvironment.run {
      httpCommand(HttpSyncOperatorAdminAppClient.CancelLogicalSynchronizerUpgrade())
    }
}

final class SyncOperatorAppClientReference(
    override val spliceConsoleEnvironment: SpliceConsoleEnvironment,
    name: String,
    val config: SyncOperatorAppClientConfig,
) extends SyncOperatorAppReference(spliceConsoleEnvironment, name) {

  override protected val instanceType = "Sync Operator Client"

  override def httpClientConfig = config.adminApi
}

final class SyncOperatorAppBackendReference(
    override val consoleEnvironment: SpliceConsoleEnvironment,
    name: String,
) extends SyncOperatorAppReference(consoleEnvironment, name)
    with AppBackendReference
    with BaseInspection[SyncOperatorApp] {

  override def runningNode: Option[SyncOperatorAppBootstrap] =
    consoleEnvironment.environment.syncOperators.getRunning(name)

  override def startingNode: Option[SyncOperatorAppBootstrap] =
    consoleEnvironment.environment.syncOperators.getStarting(name)

  override protected val instanceType = "Sync Operator Backend"

  override def httpClientConfig = NetworkAppClientConfig(
    s"http://127.0.0.1:${config.clientAdminApi.port}"
  )

  override val nodes: org.lfdecentralizedtrust.splice.environment.SyncOperatorApps =
    consoleEnvironment.environment.syncOperators

  @Help.Summary(
    "Returns the state of this app. May only be called while the app is running."
  )
  def appState: SyncOperatorApp.State = _appState[SyncOperatorApp.State, SyncOperatorApp]

  @Help.Summary(
    "Returns the automation service for the sync operator app. May only be called while the app is running."
  )
  def syncOperatorAutomation: SyncOperatorAutomationService = appState.automation

  @Help.Summary("Return local sync operator app config")
  def config: SyncOperatorAppBackendConfig =
    consoleEnvironment.environment.config.syncOperatorsByString(name)
}
