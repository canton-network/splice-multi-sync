// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import cats.syntax.either.*
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.http.v0.definitions.TrafficReportInterval
import org.lfdecentralizedtrust.splice.syncoperator.TrafficReportReader.{
  UpgradeTimes,
  cannotProvide,
  notOver,
}

import java.time.ZoneOffset
import scala.concurrent.{ExecutionContext, Future}

/** Reads every participant's cumulative consumed traffic from the sequencer's traffic journal at
  * both ends of each interval of the traffic report.
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
    synchronizerId: SynchronizerId,
    current: SequencerAdminConnection,
    successor: Option[SequencerAdminConnection],
)(implicit ec: ExecutionContext) {

  /** Reports the intervals that start at `starts`, which are consecutive, or says why the sequencer
    * can't serve all of them.
    */
  def read(
      starts: Seq[CantonTimestamp]
  )(implicit tc: TraceContext): Future[Either[String, Seq[TrafficReportInterval]]] =
    for {
      upgrades <- upgradeTimes()
      before <- unreadable(upgrades, starts)
      intervals <- before.fold(readIntervals(upgrades, starts.toList))(error =>
        Future.successful(Left(error))
      )
      // Canton prunes the journal before it moves the bound, so a prune that moved the bound
      // during the read may have removed rows the readings used.
      after <- if (intervals.isRight) unreadable(upgrades, starts) else Future.successful(None)
    } yield after.fold(intervals)(Left(_))

  /** Why the sequencer can't serve `starts`, if it pruned the first one or the first one predates
    * the upgrade to it.
    */
  private def unreadable(upgrades: UpgradeTimes, starts: Seq[CantonTimestamp])(implicit
      tc: TraceContext
  ): Future[Option[String]] =
    current.getSequencerPruningStatus().map { pruningStatus =>
      val firstReadable = report.firstReadableStart(
        upgrades.currentFrom.fold(pruningStatus.lowerBound)(_.max(pruningStatus.lowerBound))
      )
      Option.when(starts.headOption.exists(_ < firstReadable))(cannotProvide(firstReadable))
    }

  private def readIntervals(
      upgrades: UpgradeTimes,
      starts: List[CantonTimestamp],
  )(implicit tc: TraceContext): Future[Either[String, Seq[TrafficReportInterval]]] =
    starts.headOption
      .fold(Future.successful(List.empty[Long]))(first =>
        readTrafficStates(upgrades, first).flatMap {
          case None => Future.successful(List.empty)
          case Some(atFirst) => readConsumed(upgrades, starts, atFirst)
        }
      )
      .map(consumed =>
        starts.drop(consumed.size).headOption match {
          case Some(start) => Left(notOver(start, start.plus(report.intervalLength)))
          case None =>
            Right(starts.zip(consumed).map { case (start, trafficConsumed) =>
              interval(start, trafficConsumed)
            })
        }
      )

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

  private def interval(start: CantonTimestamp, trafficConsumed: Long) =
    TrafficReportInterval(
      startRecordTime = start.toInstant.atOffset(ZoneOffset.UTC),
      endRecordTime = start.plus(report.intervalLength).toInstant.atOffset(ZoneOffset.UTC),
      trafficConsumed = trafficConsumed,
    )
}

object TrafficReportReader {
  def cannotProvide(firstReadable: CantonTimestamp): String =
    s"The sequencer pruned the intervals before $firstReadable, or they predate the upgrade to " +
      s"it. Request a range that starts at or after $firstReadable."

  def notOver(start: CantonTimestamp, end: CantonTimestamp): String =
    s"The interval [$start, $end) is not over yet, or the sequencer has not sequenced past its " +
      s"end. Request a range that ends at or before $start (or wait for long enough after " +
      s"$end and retry)."

  private final case class UpgradeTimes(
      currentFrom: Option[CantonTimestamp],
      successorFrom: Option[CantonTimestamp],
  )
}
