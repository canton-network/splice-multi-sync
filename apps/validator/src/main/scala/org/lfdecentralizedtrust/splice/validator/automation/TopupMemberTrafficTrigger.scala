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
import org.lfdecentralizedtrust.splice.wallet.UserWalletManager
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
      budget <- TopupUtil.topupBudget(scanConnection, validatorWallet.store)
      candidates <- MonadUtil.sequentialTraverse(targets)(target =>
        // The traversal is sequential, so a failure would otherwise cost every remaining target
        // its top-up for this poll. Info, not warn: a synchronizer is legitimately unreachable
        // for a stretch of polls while it is being upgraded or recovered.
        retrieveTaskFor(target, activeSynchronizerId).recover { case ex =>
          logger.info(s"Skipping the top-up for ${target.alias} in this poll", ex)
          Seq.empty
        }
      )
      (funded, unfunded) = TopupMemberTrafficTrigger.fundedTasks(
        candidates.filter(_.nonEmpty),
        budget,
      )
      // we do not even submit the topup tx if the validator does not have sufficient funds because we know
      // the tx would fail but it would still drain synchronizer traffic which we would like to avoid (see #11915).
      _ = unfunded.foreach(task =>
        logger.warn(
          s"Insufficient funds to buy configured traffic amount. Please ensure that the validator's wallet has enough amulets to purchase " +
            s"${BigDecimal(task.trafficToBuy) / 1e6} MB of traffic on ${task.target.alias} to continue healthy operation."
        )
      )
      _ = funded
        .filterNot(_.withShortfall)
        .foreach(task =>
          logger.warn(
            s"Insufficient funds to buy the ${BigDecimal(task.shortfall) / 1e6} MB of traffic this member is below zero on ${task.target.alias} " +
              "together with the configured top-up, so only the top-up is bought, and the member cannot submit there until its top-ups have covered it. " +
              "Please ensure that the validator's wallet has enough amulets to buy both."
          )
        )
    } yield funded
  }

  /** The purchases this target is due, preferred first, each paired with what it would cost the
    * wallet: for a member below zero, its shortfall together with the configured top-up, and the
    * top-up alone in case the wallet does not cover both.
    */
  private def retrieveTaskFor(
      target: TopupMemberTrafficTrigger.Target,
      submissionSynchronizerId: SynchronizerId,
  )(implicit
      tc: TraceContext
  ): Future[Seq[(TopupMemberTrafficTrigger.Task, BigDecimal)]] =
    for {
      currentTrafficState <- participantAdminConnection.getParticipantTrafficState(
        target.synchronizerId
      )
      registration <-
        if (!target.needsRegistration) Future.successful(None)
        else scanConnection.lookupSynchronizerRegistration(target.synchronizerId.toProtoPrimitive)
      purchases <-
        if (target.needsRegistration && registration.isEmpty) {
          // A buy without it is rejected on-ledger, and lastPurchasedAt never advances, so
          // nothing would stop the retry.
          logger.info(
            s"Not topping up ${target.alias}: Scan serves no registration for ${target.synchronizerId}"
          )
          Future.successful(Seq.empty)
        } else
          for {
            topupState <- getOrCreateValidatorTopupState(target, submissionSynchronizerId)
            tasks <-
              if (!wantsTopup(target, currentTrafficState, topupState)) Future.successful(Seq.empty)
              else {
                val shortfall =
                  TopupMemberTrafficTrigger.shortfall(currentTrafficState.extraTrafficRemainder)
                // The top-up alone still pays a shortfall down, one interval at a time.
                val withShortfallOptions = if (shortfall > 0) Seq(true, false) else Seq(true)
                Future.traverse(withShortfallOptions)(withShortfall =>
                  TopupUtil
                    // The cost is derived from this target's throughput and interval, plus the
                    // shortfall where the purchase covers it.
                    .minWalletBalanceForTopup(
                      scanConnection,
                      target.topupConfig,
                      clock,
                      registration,
                      extraTraffic = if (withShortfall) shortfall else 0L,
                    )
                    .map(
                      TopupMemberTrafficTrigger.Task(
                        target,
                        topupState,
                        currentTrafficState,
                        registration,
                        withShortfall,
                      ) -> _
                    )
                )
              }
          } yield tasks
    } yield purchases

  override protected def completeTask(
      task: TopupMemberTrafficTrigger.Task
  )(implicit tc: TraceContext): Future[TaskOutcome] = {
    val coBuyMemberTraffic = new CO_BuyMemberTraffic(
      task.trafficToBuy,
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

  /** Whether the target is due a top-up, before the wallet balance is taken into account. */
  private def wantsTopup(
      target: TopupMemberTrafficTrigger.Target,
      currentTrafficState: TrafficState,
      topupState: Contract[ValidatorTopUpState.ContractId, ValidatorTopUpState],
  )(implicit traceContext: TraceContext): Boolean = {
    val topupParameters = target.topupParameters
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

  /** Splits candidates into the purchases the balance covers and the candidates it does not, in
    * target order. Each candidate lists its purchases preferred first and gets the first one the
    * balance covers; one it covers none of is reported by its last, cheapest, purchase. Every buy
    * draws on the same wallet, so a target is only funded once the ones before it are. A `None`
    * budget does not bound the purchases.
    */
  private[automation] def fundedTasks[A](
      candidates: Seq[Seq[(A, BigDecimal)]],
      budget: Option[BigDecimal],
  ): (Seq[A], Seq[A]) = budget match {
    case None => (candidates.flatMap(_.headOption.map(_._1)), Seq.empty)
    case Some(balance) =>
      val (funded, unfunded, _) =
        candidates.foldLeft((Seq.empty[A], Seq.empty[A], balance)) {
          case ((funded, unfunded, remaining), purchases) =>
            purchases.find { case (_, cost) => cost <= remaining } match {
              case Some((task, cost)) => (funded :+ task, unfunded, remaining - cost)
              case None => (funded, unfunded ++ purchases.lastOption.map(_._1), remaining)
            }
        }
      (funded, unfunded)
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

  /** Traffic below zero, as once a dedicated synchronizer's operator removes an outage traffic
    * allowance the member used. The member cannot submit until its purchases cover it.
    */
  def shortfall(extraTrafficRemainder: Long): Long = math.max(0L, -extraTrafficRemainder)

  /** The configured top-up plus any shortfall, bought in one purchase: the configured amount alone
    * would leave the member blocked until several top-ups had covered the shortfall.
    */
  def trafficToBuy(topupAmount: Long, extraTrafficRemainder: Long): Long =
    topupAmount + shortfall(extraTrafficRemainder)

  final case class Task(
      target: Target,
      topupState: Contract[ValidatorTopUpState.ContractId, ValidatorTopUpState],
      trafficState: TrafficState,
      registration: Option[
        ContractWithState[RegisteredSynchronizer.ContractId, RegisteredSynchronizer]
      ],
      // Whether the purchase covers the member's shortfall as well as the configured top-up. False
      // only where the wallet does not cover both, so the top-up alone pays the shortfall down.
      withShortfall: Boolean,
  ) extends PrettyPrinting {

    def shortfall: Long = TopupMemberTrafficTrigger.shortfall(trafficState.extraTrafficRemainder)

    def trafficToBuy: Long =
      if (withShortfall)
        TopupMemberTrafficTrigger.trafficToBuy(
          target.topupParameters.topupAmount,
          trafficState.extraTrafficRemainder,
        )
      else target.topupParameters.topupAmount

    override def pretty: Pretty[Task] =
      prettyOfClass[Task](
        param("target", _.target),
        param("topupState", _.topupState),
        param("trafficState", _.trafficState),
        param("withShortfall", _.withShortfall),
      )
  }
}

final case class OutOfTrafficCredit()
    extends QuietNonRetryableException("Member does not have sufficient traffic credit available")
