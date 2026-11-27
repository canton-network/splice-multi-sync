// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.sv.automation.singlesv

import com.daml.grpc.adapter.ExecutionSequencerFactory
import org.lfdecentralizedtrust.splice.automation.{SequencerPruningTriggerBase, TriggerContext}
import org.lfdecentralizedtrust.splice.environment.{
  MediatorAdminConnection,
  ParticipantAdminConnection,
  SequencerAdminConnection,
}
import org.lfdecentralizedtrust.splice.sv.store.SvDsoStore
import org.lfdecentralizedtrust.splice.util.TemplateJsonDecoder
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.ShowUtil.*
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.admin.api.client.GrpcClientMetrics
import org.lfdecentralizedtrust.splice.config.{NetworkAppClientConfig, UpgradesConfig}
import org.lfdecentralizedtrust.splice.http.HttpClient
import org.lfdecentralizedtrust.splice.scan.admin.api.client.{
  BackfillingScanConnection,
  ScanConnection,
}
import org.lfdecentralizedtrust.splice.scan.config.ScanAppClientConfig
import org.lfdecentralizedtrust.splice.scan.mediator.MediatorVerdictsClient
import org.lfdecentralizedtrust.splice.sv.config.SvScanConfig

import scala.concurrent.{ExecutionContextExecutor, Future}

/** A trigger to periodically call the sequencer pruning command
  */
class SequencerPruningTrigger(
    override protected val context: TriggerContext,
    store: SvDsoStore,
    scanConfig: SvScanConfig,
    upgradesConfig: UpgradesConfig,
    sequencerAdminConnection: SequencerAdminConnection,
    mediatorAdminConnection: MediatorAdminConnection,
    clock: Clock,
    retentionPeriod: NonNegativeFiniteDuration,
    pruningSafetyCheckPercentage: Double,
    participantAdminConnection: ParticipantAdminConnection,
    migrationId: Long,
    grpcClientMetrics: GrpcClientMetrics,
)(implicit
    override val ec: ExecutionContextExecutor,
    override val tracer: Tracer,
    mat: Materializer,
    httpClient: HttpClient,
    templateDecoder: TemplateJsonDecoder,
    esf: ExecutionSequencerFactory,
) extends SequencerPruningTriggerBase(retentionPeriod) {

  private lazy val currentMediatorClient =
    new MediatorVerdictsClient(
      mediatorAdminConnection.config,
      this,
      grpcClientMetrics,
      loggerFactory,
    )(ec, esf)

  // Similar to PublishScanConfigTrigger, this class creates its own scan connection
  // on demand, because scan might not be available at application startup.
  private def createScanConnection()(implicit
      tc: TraceContext
  ): Future[BackfillingScanConnection] =
    ScanConnection
      .singleUncached(
        ScanAppClientConfig(NetworkAppClientConfig(scanConfig.internalUrl)),
        upgradesConfig,
        clock,
        context.retryProvider,
        loggerFactory,
        retryConnectionOnInitialFailure = true,
      )

  override protected def sequencerToPrune()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection] = Future.successful(sequencerAdminConnection)

  override protected def readyToPrune(sequencer: SequencerAdminConnection)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    for {
      synchronizerId <- sequencer.getStatus.map(_.trySuccess.synchronizerId)
      scanConnection <- createScanConnection()
      // while sequencer pruning status does have a `now` timestamp that one is
      // just wallclock so doesn't work properly with backup/restore and similar stuff.
      // Therefore we just use the last ingested update as an approximation of the latest time.
      // TODO(DACH-NY/cn-test-failures#8065) Remove once Canton transfers safe pruning timestamps on LSU.
      highestRecordTimeO <- scanConnection
        .getMigrationInfo(migrationId)
        .map(_.flatMap(_.recordTimeRange.get(synchronizerId.logical).map(_.max)))
      lowestRecordTimeO <- currentMediatorClient.earliestUnprunedRecordTime()
      ready <- (lowestRecordTimeO, highestRecordTimeO) match {
        case (Some(min), Some(max))
            if (max - min).compareTo((retentionPeriod * pruningSafetyCheckPercentage).asJava) > 0 =>
          store.getDsoRulesWithSvNodeState(store.key.svParty).map { rulesAndState =>
            val dsoRulesActiveSequencerConfig = rulesAndState.lookupActiveSequencerIdConfigFor(
              rulesAndState.dsoRules.domain,
              clock.now.toInstant,
            )
            if (dsoRulesActiveSequencerConfig.isEmpty)
              logger.debug(
                show"Member info or sequencer info not (yet) published to DsoRules for our own party ${store.key.svParty}, skipping"
              )
            dsoRulesActiveSequencerConfig.nonEmpty
          }
        case _ =>
          logger.debug(
            s"Synchronizer on migration id $migrationId does not yet have $retentionPeriod of data, record time range: ${lowestRecordTimeO}, ${highestRecordTimeO}"
          )
          Future.successful(false)
      }
    } yield ready

  override protected def ownMembers(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Seq[Member]] = for {
    participantId <- participantAdminConnection.getParticipantId()
    mediatorId <- mediatorAdminConnection.getMediatorId
    sequencerId <- sequencerAdminConnection.getSequencerId
  } yield members.filter(m =>
    Seq[Member](participantId.member, mediatorId.member, sequencerId.member).contains(m)
  )
}
