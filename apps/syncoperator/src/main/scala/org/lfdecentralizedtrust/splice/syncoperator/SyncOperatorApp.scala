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
import org.lfdecentralizedtrust.splice.syncoperator.automation.SyncOperatorAutomationService
import org.lfdecentralizedtrust.splice.syncoperator.config.SyncOperatorAppBackendConfig
import org.lfdecentralizedtrust.splice.syncoperator.metrics.SyncOperatorAppMetrics
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore
import org.lfdecentralizedtrust.splice.util.HasHealth

import scala.concurrent.{ExecutionContextExecutor, Future}

/** Class representing a sync operator app instance.
  *
  * Ingests the traffic purchases made for the synchronizer it operates and grants them on that
  * synchronizer's sequencer.
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
        config.sequencer.adminApi,
        appParameters.loggingConfig.api,
        loggerFactory,
        metrics.grpcClientMetrics,
        retryProvider,
      )
      synchronizerId <- appInitStep("Get the synchronizer id from the sequencer") {
        servedSynchronizerId(sequencerAdminConnection)
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
        // MIGRATION_ID is frozen network-wide and logical synchronizer upgrades carry a serial id
        // instead, so the store's partition never has to move.
        0L,
        participantId,
        config.automation.ingestion,
        config.parameters.defaultLimit,
      )
      globalSynchronizerId <- appInitStep("Get the global synchronizer id") {
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

  /** The synchronizer the configured sequencer serves. Waits while it is still initializing. */
  private def servedSynchronizerId(
      sequencerAdminConnection: SequencerAdminConnection
  )(implicit traceContext: TraceContext): Future[SynchronizerId] =
    retryProvider.getValueWithRetries(
      RetryFor.WaitingOnInitDependency,
      "sync_operator_served_synchronizer_id",
      "the sequencer reports the synchronizer it serves",
      sequencerAdminConnection.getStatus.map(
        _.successOption
          .map(_.synchronizerId.logical)
          .getOrElse(
            throw Status.UNAVAILABLE
              .withDescription("Sequencer is not yet initialized")
              .asRuntimeException()
          )
      ),
      logger,
    )

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
