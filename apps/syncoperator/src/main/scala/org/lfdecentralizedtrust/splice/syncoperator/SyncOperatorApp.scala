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
import org.lfdecentralizedtrust.splice.admin.api.TraceContextDirectives.withTraceContext
import org.lfdecentralizedtrust.splice.admin.http.{AdminRoutes, HttpErrorHandler}
import org.lfdecentralizedtrust.splice.auth.{
  AuthConfig,
  AuthenticationOnlyAuthExtractor,
  HMACVerifier,
  RSAVerifier,
}
import org.lfdecentralizedtrust.splice.config.SharedSpliceAppParameters
import org.lfdecentralizedtrust.splice.environment.{
  BaseLedgerConnection,
  Node,
  RetryFor,
  SequencerAdminConnection,
  SpliceLedgerClient,
}
import org.lfdecentralizedtrust.splice.http.v0.sync_operator.SyncOperatorResource
import org.lfdecentralizedtrust.splice.syncoperator.admin.http.HttpSyncOperatorHandler
import org.lfdecentralizedtrust.splice.syncoperator.automation.SyncOperatorAutomationService
import org.lfdecentralizedtrust.splice.syncoperator.config.{
  SyncOperatorAppBackendConfig,
  SyncOperatorSynchronizerNodeConfig,
}
import org.lfdecentralizedtrust.splice.syncoperator.metrics.SyncOperatorAppMetrics
import org.lfdecentralizedtrust.splice.syncoperator.store.TrafficReportStore
import org.lfdecentralizedtrust.splice.util.HasHealth

import scala.concurrent.{ExecutionContextExecutor, Future}

/** Class representing a sync operator app instance.
  *
  * Runs the automation for the synchronizer it operates.
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
    adminRoutes: AdminRoutes,
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
    val synchronizerNode = newSynchronizerNode(config.synchronizerNodes.current)
    for {
      synchronizerId <- appInitStep("Get the synchronizer id from the sequencer") {
        servedSynchronizerId(synchronizerNode.sequencerAdminConnection)
      }
      _ <- appInitStep("Check the synchronizer runs traffic control") {
        requireTrafficControl(synchronizerNode.sequencerAdminConnection, synchronizerId)
      }
      trafficReportStore = new TrafficReportStore(storage)
      automation = new SyncOperatorAutomationService(
        config.automation,
        clock,
        retryProvider,
        synchronizerId,
        synchronizerNode,
        config.trafficControl,
        config.trafficBalanceReconciliationDelay,
        trafficReportStore,
        loggerFactory,
      )
      verifier = config.auth match {
        case AuthConfig.Hs256Unsafe(audience, secret) => new HMACVerifier(audience, secret)
        case AuthConfig.Rs256(audience, jwksUrl, connectionTimeout, readTimeout) =>
          new RSAVerifier(
            audience,
            jwksUrl,
            RSAVerifier.TimeoutsConfig(connectionTimeout, readTimeout),
          )
      }
      handler = new HttpSyncOperatorHandler(trafficReportStore, synchronizerId)
      route = withTraceContext { implicit traceContext =>
        HttpErrorHandler(loggerFactory)(traceContext) {
          SyncOperatorResource.routes(
            handler,
            operation =>
              metrics.httpServerMetrics.withMetrics("syncOperator")(operation).tflatMap { _ =>
                AuthenticationOnlyAuthExtractor(
                  verifier,
                  loggerFactory,
                  SyncOperatorApp.OAuthRealm,
                )(traceContext)(operation)
              },
          )
        }
      }
      _ = adminRoutes.updateRoute(route)
    } yield {
      SyncOperatorApp.State(
        automation,
        storage,
        synchronizerId,
        synchronizerNode,
        loggerFactory.getTracedLogger(SyncOperatorApp.State.getClass),
        timeouts,
      )
    }
  }

  private def newSynchronizerNode(
      nodeConfig: SyncOperatorSynchronizerNodeConfig
  ): SyncOperatorSynchronizerNode =
    new SyncOperatorSynchronizerNode(
      new SequencerAdminConnection(
        nodeConfig.sequencer.adminApi,
        appParameters.loggingConfig.api,
        loggerFactory,
        metrics.grpcClientMetrics,
        retryProvider,
      )
    )

  /** This app adjusts traffic control but never turns it on, which would strand members that have
    * no traffic yet. Waits, since the synchronizer's bootstrap may turn it on after the sequencer
    * is up.
    */
  private def requireTrafficControl(
      sequencerAdminConnection: SequencerAdminConnection,
      synchronizerId: SynchronizerId,
  )(implicit traceContext: TraceContext): Future[Unit] =
    retryProvider.waitUntil(
      RetryFor.WaitingOnInitDependency,
      "sync_operator_traffic_control",
      s"synchronizer $synchronizerId runs traffic control",
      sequencerAdminConnection
        .getSynchronizerParametersState(synchronizerId)
        .map(_.mapping.parameters.trafficControl)
        .flatMap {
          case Some(_) => Future.unit
          case None =>
            Future.failed(
              Status.FAILED_PRECONDITION
                .withDescription(
                  s"Synchronizer $synchronizerId does not run traffic control. Enable it on the " +
                    "synchronizer before starting the sync operator."
                )
                .asRuntimeException()
            )
        },
      logger,
    )

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
  val OAuthRealm = "splice sync operator realm"

  case class State(
      automation: SyncOperatorAutomationService,
      storage: DbStorage,
      synchronizerId: SynchronizerId,
      synchronizerNode: SyncOperatorSynchronizerNode,
      logger: TracedLogger,
      timeouts: ProcessingTimeout,
  ) extends AutoCloseable
      with HasHealth {

    /** The configured node's sequencer. */
    def sequencerAdminConnection: SequencerAdminConnection =
      synchronizerNode.sequencerAdminConnection

    override def isHealthy: Boolean = storage.isActive

    override def close(): Unit =
      LifeCycle.close(automation, storage, synchronizerNode)(logger)
  }
}
