// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import cats.syntax.either.*
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.http.v0.definitions.TrafficReportInterval
import org.lfdecentralizedtrust.splice.http.v0.definitions.TrafficReportInterval.Status
import org.lfdecentralizedtrust.splice.syncoperator.TrafficReportReader.UpgradeTimes

import java.time.ZoneOffset
import scala.concurrent.{ExecutionContext, Future}

/** Reads the traffic charged to participants in the intervals of the traffic report from the
  * sequencer's traffic journal.
  *
  * Reads every participant's cumulative consumed traffic at both ends of an interval.
  * ReconcileDedicatedSynchronizerParametersTrigger keeps the base rate at zero, so extra traffic is
  * all of it, the difference is exactly what the interval charged, and it is the same on every
  * sequencer.
  *
  * Across a logical synchronizer upgrade, readings up to the upgrade time come from the current
  * sequencer and later ones from the successor. A successor takes over the counters at the upgrade
  * but not their history, so it reports zero for every member before it.
  */
class TrafficReportReader(
    report: TrafficReport,
    clock: Clock,
    synchronizerId: SynchronizerId,
    current: SequencerAdminConnection,
    successor: Option[SequencerAdminConnection],
)(implicit ec: ExecutionContext) {

  /** Reports the intervals that start at `starts`, which are consecutive. */
  def read(
      starts: Seq[CantonTimestamp]
  )(implicit tc: TraceContext): Future[Seq[TrafficReportInterval]] =
    for {
      upgrades <- upgradeTimes()
      pruningStatus <- current.getSequencerPruningStatus()
      readableAfter = upgrades.currentFrom.fold(pruningStatus.lowerBound)(
        _.max(pruningStatus.lowerBound)
      )
      (unreadable, readable) = starts.partition(_ < report.firstReadableStart(readableAfter))
      // Every reading logs on the sequencer, so only read the intervals the app clock says are over;
      // the sequencer still refuses while its head lags behind.
      now = clock.now
      over = readable.takeWhile(_.plus(report.intervalLength) <= now)
      consumed <- over.headOption.fold(Future.successful(List.empty[Long]))(first =>
        readTrafficStates(upgrades, first).flatMap {
          case None => Future.successful(List.empty)
          case Some(atFirst) => readConsumed(upgrades, over.toList, atFirst)
        }
      )
    } yield {
      unreadable.map(interval(_, Status.CannotProvide)) ++
        over.zip(consumed).map { case (start, trafficConsumed) =>
          interval(start, Status.Ok, Some(trafficConsumed))
        } ++
        readable.drop(consumed.size).map(interval(_, Status.Undetermined))
    }

  /** The traffic consumed in each interval that starts at `starts`, up to the first one the
    * sequencer has not sequenced past yet.
    */
  private def readConsumed(
      upgrades: UpgradeTimes,
      starts: List[CantonTimestamp],
      atStart: Map[Member, TrafficState],
  )(implicit tc: TraceContext): Future[List[Long]] =
    starts match {
      case Nil => Future.successful(Nil)
      case start :: rest =>
        readTrafficStates(upgrades, start.plus(report.intervalLength)).flatMap {
          case None => Future.successful(Nil)
          case Some(atEnd) =>
            readConsumed(upgrades, rest, atEnd)
              .map(TrafficReport.participantsConsumed(atStart, atEnd) :: _)
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

  /** Every member's traffic state from just before `time`, or None while the sequencer has not
    * sequenced past `time` yet.
    */
  private def readTrafficStates(upgrades: UpgradeTimes, time: CantonTimestamp)(implicit
      tc: TraceContext
  ): Future[Option[Map[Member, TrafficState]]] = {
    val sequencer = (successor, upgrades.successorFrom) match {
      case (Some(successorConnection), Some(upgrade)) if time > upgrade => successorConnection
      case _ => current
    }
    sequencer
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

  private def interval(
      start: CantonTimestamp,
      status: Status,
      trafficConsumed: Option[Long] = None,
  ) =
    TrafficReportInterval(
      startRecordTime = start.toInstant.atOffset(ZoneOffset.UTC),
      endRecordTime = start.plus(report.intervalLength).toInstant.atOffset(ZoneOffset.UTC),
      status = status,
      trafficConsumed = trafficConsumed,
    )
}

object TrafficReportReader {
  private final case class UpgradeTimes(
      currentFrom: Option[CantonTimestamp],
      successorFrom: Option[CantonTimestamp],
  )
}
