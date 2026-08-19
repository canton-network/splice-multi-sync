// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import io.grpc.Status
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem
import org.lfdecentralizedtrust.splice.config.SharedSpliceAppParameters
import org.lfdecentralizedtrust.splice.environment.{
  BaseLedgerConnection,
  Node,
  PackageVersionSupport,
  ParticipantAdminConnection,
  RetryFor,
  SequencerAdminConnection,
  SpliceLedgerClient,
}
import org.lfdecentralizedtrust.splice.scan.admin.api.client.ScanConnection
import org.lfdecentralizedtrust.splice.store.db.DbAppStore
import org.lfdecentralizedtrust.splice.syncoperator.automation.SyncOperatorAutomationService
import org.lfdecentralizedtrust.splice.syncoperator.config.SyncOperatorAppBackendConfig
import org.lfdecentralizedtrust.splice.syncoperator.metrics.SyncOperatorAppMetrics
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore
import org.lfdecentralizedtrust.splice.util.HasHealth

import scala.concurrent.{ExecutionContextExecutor, Future}

/** Class representing a sync operator app instance.
  *
  * The operator side of Amulet-funded traffic on a dedicated synchronizer. Its participant is
  * connected both to the decentralized synchronizer, where purchases are burned and recorded, and
  * to the synchronizer it operates, whose sequencer it grants the purchased traffic on.
  *
  * Modelled after Canton's ParticipantNode class.
  */
class SyncOperatorApp(
    override val name: InstanceName,
    val config: SyncOperatorAppBackendConfig,
    val appParameters: SharedSpliceAppParameters,
    storage: DbStorage,
    override protected val clock: Clock,
    val loggerFactory: NamedLoggerFactory,
    tracerProvider: TracerProvider,
    futureSupervisor: FutureSupervisor,
    metrics: SyncOperatorAppMetrics,
)(implicit
    ac: ActorSystem,
    ec: ExecutionContextExecutor,
    esf: ExecutionSequencerFactory,
    tracer: Tracer,
) extends Node[SyncOperatorApp.State, Unit](
      config.operatorUser,
      config.participantClient,
      appParameters,
      loggerFactory,
      tracerProvider,
      futureSupervisor,
      metrics,
    ) {

  override lazy val ports = Map("admin" -> config.adminApi.port)

  override def preInitializeAfterLedgerConnection(
      connection: BaseLedgerConnection,
      ledgerClient: SpliceLedgerClient,
  )(implicit traceContext: TraceContext): Future[Unit] = Future.unit

  override def initialize(
      ledgerClient: SpliceLedgerClient,
      partyId: PartyId,
      preInitializeState: Unit,
  )(implicit traceContext: TraceContext): Future[SyncOperatorApp.State] = {
    val synchronizerId = SynchronizerId.tryFromString(config.synchronizer.synchronizerId)
    // MVP serves a single sequencer; see SyncOperatorSynchronizerConfig.
    val sequencerConfig = config.synchronizer.sequencers.headOption.getOrElse(
      throw Status.INVALID_ARGUMENT
        .withDescription("No sequencer configured for the dedicated synchronizer")
        .asRuntimeException()
    )

    for {
      scanConnection <- appInitStep(s"Get scan connection") {
        ScanConnection.singleCached(
          ledgerClient,
          config.scanClient,
          appParameters.upgradesConfig,
          clock,
          retryProvider,
          loggerFactory,
        )
      }
      participantAdminConnection = new ParticipantAdminConnection(
        config.participantClient.adminApi,
        appParameters.loggingConfig.api,
        loggerFactory,
        metrics.grpcClientMetrics,
        retryProvider,
      )
      participantId <- appInitStep("Get participant id") {
        participantAdminConnection.getParticipantId()
      }
      dsoParty <- appInitStep("Get DSO party id") { scanConnection.getDsoPartyId() }
      sequencerAdminConnection = new SequencerAdminConnection(
        sequencerConfig.adminApi,
        appParameters.loggingConfig.api,
        loggerFactory,
        metrics.grpcClientMetrics,
        retryProvider,
      )
      _ <- appInitStep(s"Wait for the sequencer serving $synchronizerId") {
        waitForSequencerServing(sequencerAdminConnection, synchronizerId)
      }
        // Resolved only because the store partitions its ingestion offsets by it. Purchases for a
      // registered synchronizer are pinned to migration id 0 and are ingested regardless of it,
      // see SyncOperatorStore.contractFilter.
      domainMigrationId <- appInitStep(s"Resolving domain migration id") {
        resolveDomainMigrationId(scanConnection)
      }
      storeKey = SyncOperatorStore.Key(
        operatorParty = partyId,
        dsoParty = dsoParty,
        synchronizerId = synchronizerId,
      )
      store = SyncOperatorStore(
        storeKey,
        storage,
        loggerFactory,
        retryProvider,
        domainMigrationId,
        participantId,
        config.automation.ingestion,
        config.parameters.defaultLimit,
      )
      globalSynchronizerId <- appInitStep("Get the decentralized synchronizer id") {
        scanConnection.getAmuletRulesDomain()(traceContext)
      }
      readOnlyLedgerConnection = ledgerClient
        .readOnlyConnection(
          this.getClass.getSimpleName,
          loggerFactory,
        )
      packageVersionSupport = PackageVersionSupport.createPackageVersionSupport(
        globalSynchronizerId,
        readOnlyLedgerConnection,
        loggerFactory,
      )
      automation = new SyncOperatorAutomationService(
        config.automation,
        clock,
        store,
        storage,
        ledgerClient,
        retryProvider,
        config.parameters,
        loggerFactory,
        packageVersionSupport,
      )
    } yield {
      SyncOperatorApp.State(
        automation,
        storage,
        store,
        scanConnection,
        participantAdminConnection,
        sequencerAdminConnection,
        loggerFactory.getTracedLogger(SyncOperatorApp.State.getClass),
        timeouts,
      )
    }
  }

  /** Fails initialization if the configured sequencer does not serve the configured synchronizer,
    * rather than leaving a miswired node to discover that one purchase at a time.
    */
  private def waitForSequencerServing(
      sequencerAdminConnection: SequencerAdminConnection,
      synchronizerId: SynchronizerId,
  )(implicit traceContext: TraceContext): Future[Unit] =
    retryProvider.waitUntil(
      RetryFor.WaitingOnInitDependency,
      "sync_operator_sequencer_serves_synchronizer",
      s"the configured sequencer serves $synchronizerId",
      sequencerAdminConnection.getStatus.map { status =>
        status.successOption.map(_.synchronizerId.logical) match {
          case Some(`synchronizerId`) => ()
          case Some(served) =>
            throw Status.FAILED_PRECONDITION
              .withDescription(
                s"The configured sequencer serves $served, but this node is configured for $synchronizerId"
              )
              .asRuntimeException()
          case None =>
            throw Status.UNAVAILABLE
              .withDescription("Sequencer is not yet initialized")
              .asRuntimeException()
        }
      },
      logger,
    )

  private def resolveDomainMigrationId(
      scanConnection: ScanConnection
  )(implicit traceContext: TraceContext): Future[Long] =
    DbAppStore.getHighestKnownMigrationId(storage).flatMap {
      case Some(migrationId) =>
        logger.info(s"Resolved domain migration id $migrationId from the local store offsets")
        Future.successful(migrationId)
      case None =>
        retryProvider.getValueWithRetries(
          RetryFor.WaitingOnInitDependency,
          "sync_operator_domain_migration_id",
          s"Wait for domain migration id to be available",
          scanConnection.getMigrationId().map { migrationId =>
            logger.info(s"Resolved domain migration id $migrationId from scan")
            migrationId
          },
          logger,
        )
    }

  protected[this] override def automationServices(st: SyncOperatorApp.State) =
    Seq(st.automation)
}

object SyncOperatorApp {
  case class State(
      automation: SyncOperatorAutomationService,
      storage: DbStorage,
      store: SyncOperatorStore,
      scanConnection: ScanConnection,
      participantAdminConnection: ParticipantAdminConnection,
      sequencerAdminConnection: SequencerAdminConnection,
      logger: TracedLogger,
      timeouts: ProcessingTimeout,
  ) extends AutoCloseable
      with HasHealth {
    override def isHealthy: Boolean = storage.isActive

    override def close(): Unit =
      LifeCycle.close(
        automation,
        storage,
        store,
        scanConnection,
        participantAdminConnection,
        sequencerAdminConnection,
      )(logger)
  }
}
