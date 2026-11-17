// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.sv.onboarding.lsu

import cats.syntax.foldable.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import org.lfdecentralizedtrust.splice.admin.api.client.GrpcClientMetrics
import org.lfdecentralizedtrust.splice.config.SpliceInstanceNamesConfig
import org.lfdecentralizedtrust.splice.environment.{
  ParticipantAdminConnection,
  RetryProvider,
  SpliceLedgerClient,
  SynchronizerNodeService,
}
import org.lfdecentralizedtrust.splice.store.DomainTimeSynchronization
import org.lfdecentralizedtrust.splice.sv.automation.{SvDsoAutomationService, SvSvAutomationService}
import org.lfdecentralizedtrust.splice.sv.config.{SvAppBackendConfig, SvOnboardingConfig}
import org.lfdecentralizedtrust.splice.lsu.RollForwardLsuNodeInitializer
import org.lfdecentralizedtrust.splice.sv.onboarding.{DsoPartyHosting, NodeInitializerUtil}
import org.lfdecentralizedtrust.splice.sv.onboarding.joining.JoiningNodeInitializer
import org.lfdecentralizedtrust.splice.sv.store.{SvDsoStore, SvSvStore}
import org.lfdecentralizedtrust.splice.sv.LocalSynchronizerNode

import scala.concurrent.{ExecutionContextExecutor, Future}

class RollForwardLsuInitializer(
    synchronizerNodeService: SynchronizerNodeService[LocalSynchronizerNode],
    override protected val config: SvAppBackendConfig,
    override protected val ledgerClient: SpliceLedgerClient,
    override protected val participantAdminConnection: ParticipantAdminConnection,
    override protected val clock: Clock,
    override protected val domainTimeSync: DomainTimeSynchronization,
    override protected val storage: DbStorage,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val retryProvider: RetryProvider,
    override protected val spliceInstanceNamesConfig: SpliceInstanceNamesConfig,
    override protected val grpcClientMetrics: GrpcClientMetrics,
    newJoiningNodeInitializer: Option[SvOnboardingConfig.JoinWithKey] => JoiningNodeInitializer,
    rollForwardConfig: SvOnboardingConfig.RollForwardLsu,
)(implicit
    ec: ExecutionContextExecutor
) extends NodeInitializerUtil {

  private val currentNode = synchronizerNodeService.nodes.current

  private val nodeInitializer = new RollForwardLsuNodeInitializer(
    synchronizerNodeService.nodes,
    config.domainMigrationDumpPath.getOrElse(
      throw new IllegalArgumentException("Domain migration dump path must be set for LSU")
    ),
    rollForwardConfig.newPhysicalSynchronizerSerial,
    rollForwardConfig.newPhysicalSynchronizerProtocolVersion,
    rollForwardConfig.exportTimes.map(times =>
      RollForwardLsuNodeInitializer.ExportTimes(
        times.topologyExportTime,
        times.trafficExportTime,
        times.upgradeTime,
      )
    ),
    clock,
    loggerFactory,
    retryProvider,
  )

  def rollForward()(implicit tc: TraceContext): Future[
    (
        SynchronizerId,
        DsoPartyHosting,
        SvSvStore,
        SvSvAutomationService,
        SvDsoStore,
        SvDsoAutomationService,
    )
  ] =
    for {
      rolledForward <- nodeInitializer.initializeCurrentNode()
      sequencerId <- currentNode.sequencerAdminConnection.getSequencerId
      participantPhysicalSynchronizerId <- participantAdminConnection.getPhysicalSynchronizerId(
        rolledForward.legacyPhysicalSynchronizerId.logical
      )
      _ <-
        if (participantPhysicalSynchronizerId == rolledForward.newPhysicalSynchronizerId) {
          logger.info("Participant already migrated")
          Future.unit
        } else {
          logger.info(
            s"Participant is on physical synchronizer id $participantPhysicalSynchronizerId, initiating manual LSU"
          )
          participantAdminConnection
            .performManualLsu(
              rolledForward.legacyPhysicalSynchronizerId,
              rolledForward.newPhysicalSynchronizerId,
              rolledForward.upgradeTime,
              Map(
                sequencerId -> nodeInitializer.successorConnection
              ),
            )
            .map { r =>
              logger.info("Manual LSU completed")
              r
            }
        }
      result @ (_, _, _, _, store, _) <- newJoiningNodeInitializer(None).joinDsoAndOnboardNodes()
      rulesAndState <- store.getDsoRulesWithSvNodeStates()
      owningNodeSvName <- rulesAndState.getSvNameInDso(store.key.svParty)
      _ <- currentNode.cometbftNode.traverse_(
        _.rotateGenesisGovernanceKeyForSV1(owningNodeSvName)
      )
      _ <- currentNode.cometbftNode.traverse_(
        _.reconcileNetworkConfig(owningNodeSvName, rulesAndState)
      )
    } yield result
}
