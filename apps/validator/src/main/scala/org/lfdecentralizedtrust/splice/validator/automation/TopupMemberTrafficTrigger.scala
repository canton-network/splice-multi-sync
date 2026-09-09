// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.validator.automation

import com.daml.grpc.{GrpcException, GrpcStatus}
import org.lfdecentralizedtrust.splice.automation.{
  PollingParallelTaskExecutionTrigger,
  TaskOutcome,
  TaskSuccess,
  TriggerContext,
}
import org.lfdecentralizedtrust.splice.codegen.java.splice.wallet.install.amuletoperation.CO_BuyMemberTraffic
import org.lfdecentralizedtrust.splice.codegen.java.splice.wallet.install.amuletoperationoutcome.{
  COO_BuyMemberTraffic,
  COO_Error,
}
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.RegisteredSynchronizer
import org.lfdecentralizedtrust.splice.codegen.java.splice.wallet.topupstate.ValidatorTopUpState
import org.lfdecentralizedtrust.splice.codegen.java.da.time.types.RelTime
import org.lfdecentralizedtrust.splice.environment.RetryProvider.QuietNonRetryableException
import org.lfdecentralizedtrust.splice.environment.ledger.api.DedupOffset
import org.lfdecentralizedtrust.splice.environment.{
  SpliceLedgerConnection,
  CommandPriority,
  ParticipantAdminConnection,
}
import org.lfdecentralizedtrust.splice.scan.admin.api.client.BftScanConnection
import org.lfdecentralizedtrust.splice.store.MultiDomainAcsStore.QueryResult
import org.lfdecentralizedtrust.splice.util.{
  AmuletConfigSchedule,
  Contract,
  ContractWithState,
  DisclosedContracts,
}
import org.lfdecentralizedtrust.splice.validator.store.ValidatorStore
import org.lfdecentralizedtrust.splice.validator.util.ValidatorUtil
import org.lfdecentralizedtrust.splice.wallet.util.{
  ExtraTrafficTopupParameters,
  TopupUtil,
  ValidatorTopupConfig,
}
import org.lfdecentralizedtrust.splice.validator.config.{
  BuyExtraTrafficConfig,
  ValidatorSynchronizerConfig,
}
import org.lfdecentralizedtrust.splice.wallet.{UserWalletManager, UserWalletService}
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.{SequencerErrors, TrafficState}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import io.grpc.Status
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.time.Instant
import java.util.Optional
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*
import scala.util.{Failure, Success}

class TopupMemberTrafficTrigger(
    override protected val context: TriggerContext,
    store: ValidatorStore,
    connection: SpliceLedgerConnection,
    participantAdminConnection: ParticipantAdminConnection,
    synchronizerConfig: ValidatorSynchronizerConfig,
    clock: Clock,
    walletManager: UserWalletManager,
    scanConnection: BftScanConnection,
    domainMigrationId: Long,
)(implicit
    override val ec: ExecutionContext,
    override val tracer: Tracer,
    mat: Materializer,
) extends PollingParallelTaskExecutionTrigger[TopupMemberTrafficTrigger.Task] {

  private val validator = store.key.validatorParty

  override protected def retrieveTasks()(implicit
      tc: TraceContext
  ): Future[Seq[TopupMemberTrafficTrigger.Task]] = {
    for {
      amuletRules <- scanConnection.getAmuletRulesWithState()
      decentralizedSynchronizerConfig = AmuletConfigSchedule(amuletRules)
        .getConfigAsOf(clock.now)
        .decentralizedSynchronizer
      // TODO(DACH-NY/canton-network-node#13301) This switches over to purchasing traffic for the new synchronizer
      // as soon as it is active. This might be sufficient for Amulet where
      // there a validator has a relatively small amount of contracts and everything is
      // forced to switch over so the remaining extra traffic + the base rate might
      // be sufficient to complete any unassign commands.
      // However for other apps that might switch over later or have much larger ACS,
      // we likely want to still allow purchasing traffic for the old synchronizer.
      activeSynchronizerId = SynchronizerId.tryFromString(
        decentralizedSynchronizerConfig.activeSynchronizer
      )
      connected <- participantAdminConnection.listConnectedSynchronizers()
      targets = TopupMemberTrafficTrigger.resolveTargets(
        topupTargets = synchronizerConfig.topupTargets,
        globalAlias = synchronizerConfig.global.alias,
        globalSynchronizerId = activeSynchronizerId,
        connectedSynchronizerIds =
          connected.map(r => r.synchronizerAlias -> r.synchronizerId).toMap,
        requiredSynchronizerIds =
          decentralizedSynchronizerConfig.requiredSynchronizers.map.keySet.asScala.toSet,
        minTopupAmount = decentralizedSynchronizerConfig.fees.minTopupAmount,
        pollingInterval = context.config.pollingInterval,
        domainMigrationId = domainMigrationId,
        logger = logger,
      )
      validatorWallet <- ValidatorUtil.getValidatorWallet(store, walletManager)
      tasks <- MonadUtil.sequentialTraverseFilter(targets)(target =>
        skipOnFailure(target, hasOtherTargets = targets.sizeIs > 1)(
          retrieveTaskFor(target, validatorWallet, activeSynchronizerId)
        )
      )
    } yield tasks
  }

  /** A failure costs every other target its top-up for that poll, so an unreachable dedicated
    * synchronizer is skipped. A failure on the decentralized synchronizer, or on a lone target,
    * stays visible: every buy is submitted there, so nothing can be bought at all.
    */
  private def skipOnFailure[A](
      target: TopupMemberTrafficTrigger.Target,
      hasOtherTargets: Boolean,
  )(task: Future[Option[A]])(implicit tc: TraceContext): Future[Option[A]] =
    task.transform {
      case Failure(ex) if target.isGlobal || !hasOtherTargets => Failure(ex)
      case Failure(ex) =>
        logger.info(s"Skipping the top-up for ${target.alias} in this poll", ex)
        Success(None)
      case success => success
    }

  private def retrieveTaskFor(
      target: TopupMemberTrafficTrigger.Target,
      validatorWallet: UserWalletService,
      submissionSynchronizerId: SynchronizerId,
  )(implicit tc: TraceContext): Future[Option[TopupMemberTrafficTrigger.Task]] =
    for {
      currentTrafficState <- participantAdminConnection.getParticipantTrafficState(
        target.synchronizerId
      )
      registration <-
        if (!target.needsRegistration) Future.successful(None)
        else scanConnection.lookupSynchronizerRegistration(target.synchronizerId.toProtoPrimitive)
      task <-
        if (target.needsRegistration && registration.isEmpty) {
          // A buy without it is rejected on-ledger, and lastPurchasedAt never advances, so
          // nothing would stop the retry.
          logger.info(
            s"Not topping up ${target.alias}: Scan serves no registration for ${target.synchronizerId}"
          )
          Future.successful(None)
        } else
          for {
            topupState <- getOrCreateValidatorTopupState(target, submissionSynchronizerId)
            hasSufficientFunds <- TopupUtil.hasSufficientFundsForTopup(
              scanConnection,
              validatorWallet.store,
              // The required balance is derived from this target's throughput and interval.
              target.topupConfig,
              clock,
            )
          } yield Option.when(
            shouldTopup(target, hasSufficientFunds, currentTrafficState, topupState)
          )(
            TopupMemberTrafficTrigger.Task(
              target,
              topupState,
              currentTrafficState,
              registration,
            )
          )
    } yield task

  override protected def completeTask(
      task: TopupMemberTrafficTrigger.Task
  )(implicit tc: TraceContext): Future[TaskOutcome] = {
    val coBuyMemberTraffic = new CO_BuyMemberTraffic(
      task.target.topupParameters.topupAmount,
      task.topupState.payload.memberId,
      task.topupState.payload.synchronizerId,
      task.topupState.payload.migrationId,
      new RelTime(task.target.topupParameters.minTopupInterval.duration.toMillis * 1000),
      Optional.of(task.topupState.contractId),
      task.registration.map(_.contractId).toJava,
    )
    for {
      validatorWallet <- ValidatorUtil.getValidatorWallet(store, walletManager)
      outcome <- validatorWallet.treasury
        .enqueueAmuletOperation(
          coBuyMemberTraffic,
          CommandPriority.High,
          // The buyer is not a stakeholder on the registration, so it has to be disclosed.
          extraDisclosedContracts = task.registration
            .fold[DisclosedContracts](DisclosedContracts.Empty)(connection.disclosedContracts(_)),
        )
        .map {
          case outcome: COO_BuyMemberTraffic =>
            TaskSuccess(s"Successfully bought extra traffic: $outcome")
          case error: COO_Error =>
            throw Status.ABORTED
              .withDescription(s"Received an unexpected COOError: $error - ignoring for now")
              .asRuntimeException()
          case otherwise =>
            throw Status.INTERNAL
              .withDescription(s"Unexpected COO return type: $otherwise")
              .asRuntimeException()
        }
        .recover {
          case GrpcException(GrpcStatus(statusCode, someDescription), _)
              if statusCode == Status.Code.FAILED_PRECONDITION && someDescription.exists(
                _.contains(SequencerErrors.TrafficCredit.id)
              ) =>
            throw OutOfTrafficCredit()
        }
    } yield outcome
  }

  override protected def isStaleTask(
      task: TopupMemberTrafficTrigger.Task
  )(implicit tc: TraceContext): Future[Boolean] = {
    for {
      currentTopupState <- store
        .lookupValidatorTopUpStateWithOffset(task.target.synchronizerId, task.target.migrationId)
        .map(_.value)
      // A registration archived by the DSO makes every retry a rejected submission. Compare
      // contract ids, so a re-registration also reads as stale.
      registrationGone <- task.registration match {
        case None => Future.successful(false)
        case Some(registration) =>
          scanConnection
            .lookupSynchronizerRegistration(registration.payload.synchronizerId)
            .map(!_.exists(_.contractId == registration.contractId))
      }
    } yield registrationGone || currentTopupState.exists(
      _.payload.lastPurchasedAt.isAfter(task.topupState.payload.lastPurchasedAt)
    )
  }

  private def shouldTopup(
      target: TopupMemberTrafficTrigger.Target,
      hasSufficientFunds: Boolean,
      currentTrafficState: TrafficState,
      topupState: Contract[ValidatorTopUpState.ContractId, ValidatorTopUpState],
  )(implicit traceContext: TraceContext): Boolean = {
    val topupParameters = target.topupParameters
    // we do not even submit the topup tx if the validator does not have sufficient funds because we know
    // the tx would fail but it would still drain synchronizer traffic which we would like to avoid (see #11915).
    if (!hasSufficientFunds) {
      logger.warn(
        s"Insufficient funds to buy configured traffic amount. Please ensure that the validator's wallet has enough amulets to purchase " +
          s"${BigDecimal(topupParameters.topupAmount) / 1e6} MB of traffic on ${target.alias} to continue healthy operation."
      )
      false
    } else {
      val currentExtraTrafficRemainder =
        currentTrafficState.extraTrafficRemainder
      val currentTime = clock.now
      val tooSoon =
        topupState.payload.lastPurchasedAt.toEpochMilli + topupParameters.minTopupInterval.duration.toMillis > currentTime.toEpochMilli
      if (tooSoon) {
        logger.trace(
          s"Trying to top-up ${target.alias} too soon after previous top-up (last purchased at = ${topupState.payload.lastPurchasedAt}, current time = $currentTime)"
        )
        false
      } else if (currentExtraTrafficRemainder >= topupParameters.topupAmount) {
        logger.trace(
          s"Sufficient traffic balance remains on ${target.alias} (current traffic balance = $currentExtraTrafficRemainder, topup amount = ${topupParameters.topupAmount})"
        )
        false
      } else {
        true
      }
    }
  }

  private def getOrCreateValidatorTopupState(
      target: TopupMemberTrafficTrigger.Target,
      submissionSynchronizerId: SynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): Future[Contract[ValidatorTopUpState.ContractId, ValidatorTopUpState]] = {
    // Keyed on the target, not on where the contract is assigned.
    store.lookupValidatorTopUpStateWithOffset(target.synchronizerId, target.migrationId).flatMap {
      case QueryResult(_, Some(topupState)) =>
        Future.successful(topupState)
      case QueryResult(dedupOffset, None) =>
        for {
          participantId <- participantAdminConnection.getParticipantId()
          topupState <- connection
            .submit(
              Seq(validator),
              Seq(validator),
              ValidatorTopUpState.create(
                store.key.dsoParty.toProtoPrimitive,
                validator.toProtoPrimitive,
                participantId.toProtoPrimitive,
                target.synchronizerId.toProtoPrimitive,
                // The buy fetches the state by this migration id, so it has to be the target's.
                target.migrationId,
                Instant.ofEpochSecond(0),
              ),
              priority = CommandPriority.High,
              deadline = target.grpcDeadline,
            )
            .withDedup(
              SpliceLedgerConnection.CommandId(
                "org.lfdecentralizedtrust.splice.validator.automation.TopupMemberTrafficTrigger.getOrCreateValidatorTopupState",
                Seq(validator),
                // The target's id, not the submission synchronizer's, which is the same for every
                // target. Kept to one element, so the existing decentralized key hashes unchanged.
                target.synchronizerId.toProtoPrimitive,
              ),
              DedupOffset(dedupOffset),
            )
            // The decentralized synchronizer, for every target: the buy infers its domain from
            // the disclosed AmuletRules and open round, so a state assigned elsewhere is
            // unfetchable in that transaction.
            .withSynchronizerId(submissionSynchronizerId)
            .yieldResult()
            .flatMap(ev =>
              store.multiDomainAcsStore.getContractByIdOnDomain(ValidatorTopUpState.COMPANION)(
                submissionSynchronizerId,
                ev.contractId,
              )
            )
        } yield topupState
    }
  }
}

object TopupMemberTrafficTrigger {

  /** One synchronizer this validator tops up, resolved against the participant's connections. */
  final case class Target(
      alias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      isGlobal: Boolean,
      // 0 for a registered synchronizer, which AmuletRules rejects at any other migration id.
      migrationId: Long,
      // False for anything in requiredSynchronizers, which AmuletRules authorizes by membership.
      needsRegistration: Boolean,
      topupConfig: ValidatorTopupConfig,
      topupParameters: ExtraTrafficTopupParameters,
      grpcDeadline: Option[NonNegativeFiniteDuration],
  ) extends PrettyPrinting {
    override def pretty: Pretty[Target] = prettyOfClass[Target](
      param("alias", _.alias),
      param("synchronizerId", _.synchronizerId),
      param("migrationId", _.migrationId),
      param("topupParameters", _.topupParameters),
    )
  }

  /** Resolves the configured top-up targets against the synchronizers the participant is
    * connected to.
    */
  private[automation] def resolveTargets(
      topupTargets: Seq[(SynchronizerAlias, BuyExtraTrafficConfig)],
      globalAlias: SynchronizerAlias,
      globalSynchronizerId: SynchronizerId,
      connectedSynchronizerIds: Map[SynchronizerAlias, SynchronizerId],
      requiredSynchronizerIds: Set[String],
      minTopupAmount: Long,
      pollingInterval: NonNegativeFiniteDuration,
      domainMigrationId: Long,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Seq[Target] =
    topupTargets
      .flatMap { case (alias, topup) =>
        val isGlobal = alias == globalAlias
        val resolved =
          if (isGlobal) Some(globalSynchronizerId) else connectedSynchronizerIds.get(alias)
        resolved match {
          case None =>
            // Expected: an extra synchronizer stays configured while the participant is not
            // connected to it.
            logger.debug(s"Not topping up $alias: the participant is not connected to it")
            None
          case Some(synchronizerId) =>
            // Never for the global target, whose migration id must follow the DSO even if
            // activeSynchronizer ever leaves requiredSynchronizers.
            val needsRegistration =
              !isGlobal && !requiredSynchronizerIds.contains(synchronizerId.toProtoPrimitive)
            val topupConfig =
              ValidatorTopupConfig(topup.targetThroughput, topup.minTopupInterval, pollingInterval)
            Some(
              Target(
                alias,
                synchronizerId,
                isGlobal = isGlobal,
                migrationId = if (needsRegistration) 0L else domainMigrationId,
                needsRegistration = needsRegistration,
                topupConfig,
                ExtraTrafficTopupParameters(topupConfig, minTopupAmount),
                topup.grpcDeadline,
              )
            )
        }
      }
      // Two aliases can resolve to one synchronizer; topupTargets is global-first, so global wins.
      .distinctBy(_.synchronizerId)

  final case class Task(
      target: Target,
      topupState: Contract[ValidatorTopUpState.ContractId, ValidatorTopUpState],
      trafficState: TrafficState,
      registration: Option[
        ContractWithState[RegisteredSynchronizer.ContractId, RegisteredSynchronizer]
      ],
  ) extends PrettyPrinting {
    override def pretty: Pretty[Task] =
      prettyOfClass[Task](
        param("target", _.target),
        param("topupState", _.topupState),
        param("trafficState", _.trafficState),
      )
  }
}

final case class OutOfTrafficCredit()
    extends QuietNonRetryableException("Member does not have sufficient traffic credit available")
