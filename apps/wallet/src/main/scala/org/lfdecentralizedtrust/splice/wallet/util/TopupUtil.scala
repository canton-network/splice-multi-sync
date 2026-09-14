// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.wallet.util

import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.{
  AmuletDecentralizedSynchronizerConfig,
  RegisteredSynchronizer,
}
import org.lfdecentralizedtrust.splice.scan.admin.api.client.ScanConnection
import org.lfdecentralizedtrust.splice.util.{AmuletConfigSchedule, ContractWithState, SpliceUtil}
import org.lfdecentralizedtrust.splice.wallet.store.UserWalletStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

object TopupUtil {
  def minWalletBalanceForTopup(
      scanConnection: ScanConnection,
      validatorTopupConfig: ValidatorTopupConfig,
      clock: Clock,
  )(implicit tc: TraceContext, ec: ExecutionContext, mat: Materializer): Future[BigDecimal] = for {
    amuletRules <- scanConnection.getAmuletRulesWithState()
    synchronizerFeesConfig = AmuletConfigSchedule(amuletRules)
      .getConfigAsOf(clock.now)
      .decentralizedSynchronizer
      .fees
    topupParameters = ExtraTrafficTopupParameters(
      validatorTopupConfig.targetThroughput,
      validatorTopupConfig.minTopupInterval,
      synchronizerFeesConfig.minTopupAmount,
      validatorTopupConfig.topupTriggerPollingInterval,
    )
    latestRound <- scanConnection.getLatestOpenMiningRound()
    amuletPrice = latestRound.payload.amuletPrice
    extraTrafficPrice = BigDecimal(synchronizerFeesConfig.extraTrafficPrice)
  } yield SpliceUtil
    .synchronizerFees(topupParameters.topupAmount, extraTrafficPrice, amuletPrice)
    ._2

  private def currentWalletBalance(scanConnection: ScanConnection, store: UserWalletStore)(implicit
      tc: TraceContext,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[BigDecimal] = for {
    latestRound <- scanConnection.getLatestOpenMiningRound()
    roundNum = latestRound.payload.round.number
    walletBalance <- store
      .getAmuletBalanceWithHoldingFees(
        roundNum
      )
      .map(_._1)
  } yield walletBalance

  /** The balance the wallet can spend on traffic, or `None` where it does not bound the purchase. */
  def topupBudget(
      scanConnection: ScanConnection,
      validatorWalletStore: UserWalletStore,
  )(implicit
      tc: TraceContext,
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[BigDecimal]] = {
    scanConnection.getAmuletRulesWithState().flatMap { amuletRules =>
      // Since we auto-tap CC for traffic purchases on DevNet, we always have sufficient funds
      // TODO(#851): Considering removing this once we remove auto-tapping in DevNet
      if (amuletRules.payload.isDevNet) Future.successful(None)
      else currentWalletBalance(scanConnection, validatorWalletStore).map(Some(_))
    }
  }

  def hasSufficientFundsForTopup(
      scanConnection: ScanConnection,
      validatorWalletStore: UserWalletStore,
      validatorTopupConfig: ValidatorTopupConfig,
      clock: Clock,
  )(implicit tc: TraceContext, ec: ExecutionContext, mat: Materializer): Future[Boolean] = {
    topupBudget(scanConnection, validatorWalletStore).flatMap {
      case None => Future.successful(true)
      case Some(walletBalance) =>
        minWalletBalanceForTopup(scanConnection, validatorTopupConfig, clock)
          .map(walletBalance >= _)
    }
  }

  /** How AmuletRules authorizes a traffic purchase on a synchronizer. */
  sealed trait TrafficAuthorization
  object TrafficAuthorization {

    /** Listed in `requiredSynchronizers`: bought at the validator's migration id, without a
      * registration.
      */
    case object Required extends TrafficAuthorization

    /** Authorized by a `RegisteredSynchronizer` disclosed with the purchase, at migration id 0. */
    final case class Registered(
        registration: ContractWithState[RegisteredSynchronizer.ContractId, RegisteredSynchronizer]
    ) extends TrafficAuthorization

    /** Neither required nor registered: no purchase can succeed. */
    case object Unknown extends TrafficAuthorization
  }

  def trafficAuthorization(
      scanConnection: ScanConnection,
      decentralizedSynchronizerConfig: AmuletDecentralizedSynchronizerConfig,
      synchronizerId: String,
  )(implicit tc: TraceContext, ec: ExecutionContext): Future[TrafficAuthorization] =
    if (decentralizedSynchronizerConfig.requiredSynchronizers.map.containsKey(synchronizerId))
      Future.successful(TrafficAuthorization.Required)
    else
      scanConnection.lookupSynchronizerRegistration(synchronizerId).map {
        case Some(registration) => TrafficAuthorization.Registered(registration)
        case None => TrafficAuthorization.Unknown
      }

}
