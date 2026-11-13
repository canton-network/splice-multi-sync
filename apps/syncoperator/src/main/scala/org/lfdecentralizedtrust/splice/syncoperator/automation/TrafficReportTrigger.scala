// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import cats.syntax.either.*
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import org.lfdecentralizedtrust.splice.automation.{PollingTrigger, TriggerContext}
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.syncoperator.TrafficReport
import org.lfdecentralizedtrust.splice.syncoperator.automation.TrafficReportTrigger.UpgradeTimes
import org.lfdecentralizedtrust.splice.syncoperator.store.TrafficReportStore

import scala.concurrent.{ExecutionContext, Future}

/** Stores the traffic charged to participants in each interval of the traffic report once it is
  * over.
  *
  * Reads every participant's cumulative consumed traffic at both ends of the interval from the
  * sequencer's traffic journal. ReconcileDedicatedSynchronizerParametersTrigger keeps the base
  * rate at zero, so extra traffic is all of it, the difference is exactly what the interval
  * charged, and it is the same on every sequencer.
  *
  * Across a logical synchronizer upgrade, readings up to the upgrade time come from the current
  * sequencer and later ones from the successor. A successor takes over the counters at the upgrade
  * but not their history, so it reports zero for every member before it.
  */
class TrafficReportTrigger(
    override protected val context: TriggerContext,
    synchronizerId: SynchronizerId,
    current: SequencerAdminConnection,
    successor: Option[SequencerAdminConnection],
    store: TrafficReportStore,
)(implicit
    override val ec: ExecutionContext,
    override val tracer: Tracer,
) extends PollingTrigger {

  override def performWorkIfAvailable()(implicit tc: TraceContext): Future[Boolean] =
    store.lookupLatestIntervalStart().flatMap { latest =>
      val now = context.clock.now
      // On first start, take the last interval that is over rather than backfilling to the pruning
      // lower bound, which is the epoch on a sequencer that never pruned.
      val next = latest.fold(TrafficReport.alignDown(now).minus(TrafficReport.IntervalLength))(
        _.plus(TrafficReport.IntervalLength)
      )
      // Every call logs on the sequencer, so only call once the app clock says the interval is
      // over; the sequencer still refuses while its head lags behind.
      if (next.plus(TrafficReport.IntervalLength) > now) Future.successful(false)
      else
        for {
          upgrades <- upgradeTimes()
          pruningStatus <- sequencerAt(upgrades, next).getSequencerPruningStatus()
          readableAfter = upgrades.currentFrom.fold(pruningStatus.lowerBound)(
            _.max(pruningStatus.lowerBound)
          )
          start = next.max(TrafficReport.firstReadableStart(readableAfter))
          stored <- storeInterval(upgrades, start)
        } yield {
          if (stored && latest.isDefined && start > next)
            logger.warn(
              s"The traffic of the intervals from $next to $start was pruned or predates the " +
                "upgrade to this sequencer, so the traffic report cannot provide them"
            )
          stored
        }
    }

  private def storeInterval(upgrades: UpgradeTimes, start: CantonTimestamp)(implicit
      tc: TraceContext
  ): Future[Boolean] = {
    val end = start.plus(TrafficReport.IntervalLength)
    readTrafficStates(sequencerAt(upgrades, end), end).flatMap {
      case None => Future.successful(false)
      case Some(atEnd) =>
        readTrafficStates(sequencerAt(upgrades, start), start).flatMap {
          case None => Future.successful(false)
          case Some(atStart) =>
            store
              .insertInterval(start, TrafficReport.participantsConsumed(atStart, atEnd))
              .map(_ => true)
        }
    }
  }

  /** The current sequencer's topology still lists the upgrade that made it current. */
  private def upgradeTimes()(implicit tc: TraceContext): Future[UpgradeTimes] =
    for {
      psid <- current.getPhysicalSynchronizerId()
      announcements <- current.listLsuAnnouncements(synchronizerId)
    } yield {
      val upgrades = announcements.map(_.mapping)
      UpgradeTimes(
        currentFrom =
          upgrades.find(_.successorSynchronizerId.serial == psid.serial).map(_.upgradeTime),
        successorFrom =
          upgrades.find(_.successorSynchronizerId.serial > psid.serial).map(_.upgradeTime),
      )
    }

  private def sequencerAt(upgrades: UpgradeTimes, time: CantonTimestamp): SequencerAdminConnection =
    (successor, upgrades.successorFrom) match {
      case (Some(successorConnection), Some(upgrade)) if time > upgrade => successorConnection
      case _ => current
    }

  /** Every member's traffic state from just before `time`, or None while the sequencer has not
    * sequenced past `time` yet.
    */
  private def readTrafficStates(
      connection: SequencerAdminConnection,
      time: CantonTimestamp,
  )(implicit tc: TraceContext): Future[Option[Map[Member, TrafficState]]] =
    connection
      .getLsuTrafficControlState(Some(time))
      .map(bytes =>
        Some(
          LsuTrafficState
            .fromTrustedByteString(bytes)
            .valueOr(error =>
              throw new IllegalStateException(
                s"Failed to parse the traffic states at $time: $error"
              )
            )
            .membersTraffic
        )
      )
      .recover {
        case e: StatusRuntimeException
            if ErrorDetails.matches(e, SequencerError.NotAtUpgradeTimeOrBeyond) =>
          None
      }
}

object TrafficReportTrigger {
  private final case class UpgradeTimes(
      currentFrom: Option[CantonTimestamp],
      successorFrom: Option[CantonTimestamp],
  )
}
