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
import org.lfdecentralizedtrust.splice.syncoperator.TrafficReportReader.{cannotProvide, notOver}

import java.time.{Duration, ZoneOffset}
import scala.concurrent.{ExecutionContext, Future}

/** Reads every participant's cumulative consumed traffic from the sequencer's traffic journal at
  * both ends of each interval of the traffic report.
  * ReconcileDedicatedSynchronizerParametersTrigger keeps the base rate at zero, so extra traffic is
  * all of it, the difference is exactly what the interval charged, and it is the same on every
  * sequencer.
  *
  * Across a logical synchronizer upgrade, readings up to the upgrade time come from the current
  * sequencer and later ones from the successor. A successor takes over each member's counter at
  * the upgrade but not the history before it.
  */
class TrafficReportReader(
    report: TrafficReport,
    synchronizerId: SynchronizerId,
    current: SequencerAdminConnection,
    successor: Option[SequencerAdminConnection],
    retentionPeriod: Option[Duration],
)(implicit ec: ExecutionContext) {

  /** Reports the intervals that start at `starts`, which are consecutive, or says why the sequencer
    * can't serve all of them.
    */
  def read(
      starts: Seq[CantonTimestamp]
  )(implicit tc: TraceContext): Future[Either[String, Seq[TrafficReportInterval]]] =
    for {
      upgrades <- UpgradeTimes.of(current, synchronizerId)
      before <- unreadable(upgrades, starts)
      intervals <- before.fold(readIntervals(upgrades, starts.toList))(error =>
        Future.successful(Left(error))
      )
      // Canton prunes the journal before it moves the bound, so check again in case the retention
      // period moved past the first reading while a prune ran.
      after <- if (intervals.isRight) unreadable(upgrades, starts) else Future.successful(None)
    } yield after.fold(intervals)(Left(_))

  /** Why the sequencers can't serve `starts`: one of them pruned the first reading it serves or may
    * prune it, as it is older than the retention period, or that reading predates the upgrade to
    * the current sequencer.
    */
  private def unreadable(upgrades: UpgradeTimes, starts: Seq[CantonTimestamp])(implicit
      tc: TraceContext
  ): Future[Option[String]] = {
    val readings = starts ++ starts.lastOption.map(_.plus(report.intervalLength))
    val (fromSuccessor, fromCurrent) = readings.partition(successorAt(upgrades, _).isDefined)
    val checks =
      fromCurrent.headOption.map(unreadableAt(current, upgrades.currentFrom, _)).toList ++
        successor.zip(fromSuccessor.headOption).map { case (successorConnection, first) =>
          unreadableAt(successorConnection, None, first)
        }
    Future.sequence(checks).map(_.flatten.headOption)
  }

  /** Why `sequencer` can't serve a reading at `first`: it pruned it or may prune it, or it predates
    * `from`, the upgrade to that sequencer.
    */
  private def unreadableAt(
      sequencer: SequencerAdminConnection,
      from: Option[CantonTimestamp],
      first: CantonTimestamp,
  )(implicit tc: TraceContext): Future[Option[String]] =
    sequencer.getSequencerPruningStatus().map { pruningStatus =>
      val bound = (from.toList ++ retentionPeriod.map(pruningStatus.now.minus))
        .foldLeft(pruningStatus.lowerBound)(_ max _)
      Option.when(first <= bound)(cannotProvide(report.firstReadableStart(bound)))
    }

  /** The successor, for a reading after the upgrade to it. Other readings come from the current
    * sequencer.
    */
  private def successorAt(
      upgrades: UpgradeTimes,
      time: CantonTimestamp,
  ): Option[SequencerAdminConnection] =
    successor.filter(_ => upgrades.successorFrom.exists(time > _))

  private def readIntervals(
      upgrades: UpgradeTimes,
      starts: List[CantonTimestamp],
  )(implicit tc: TraceContext): Future[Either[String, Seq[TrafficReportInterval]]] =
    starts.headOption
      .fold(Future.successful[Either[String, List[Long]]](Right(Nil)))(first =>
        readTrafficStates(upgrades, first).flatMap {
          case None => Future.successful(Left(notOver(first, first.plus(report.intervalLength))))
          case Some(atFirst) => readConsumed(upgrades, starts, atFirst)
        }
      )
      .map(
        _.map(consumed =>
          starts.zip(consumed).map { case (start, trafficConsumed) =>
            interval(start, trafficConsumed)
          }
        )
      )

  /** The traffic consumed in each interval that starts at `starts`, or why the sequencer can't
    * serve the first one it fails on.
    */
  private def readConsumed(
      upgrades: UpgradeTimes,
      starts: List[CantonTimestamp],
      atStart: Map[Member, TrafficState],
  )(implicit tc: TraceContext): Future[Either[String, List[Long]]] =
    starts match {
      case Nil => Future.successful(Right(Nil))
      case start :: rest =>
        val end = start.plus(report.intervalLength)
        readTrafficStates(upgrades, end).flatMap {
          case None => Future.successful(Left(notOver(start, end)))
          case Some(atEnd) =>
            readConsumed(upgrades, rest, atEnd)
              .map(_.map(TrafficReport.participantsConsumed(atStart, atEnd) :: _))
        }
    }

  /** Every member's traffic state from just before `time`, or None while the sequencer has not
    * sequenced past `time` yet.
    */
  private def readTrafficStates(upgrades: UpgradeTimes, time: CantonTimestamp)(implicit
      tc: TraceContext
  ): Future[Option[Map[Member, TrafficState]]] =
    successorAt(upgrades, time)
      .getOrElse(current)
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

  private def interval(start: CantonTimestamp, trafficConsumed: Long) =
    TrafficReportInterval(
      startRecordTime = start.toInstant.atOffset(ZoneOffset.UTC),
      endRecordTime = start.plus(report.intervalLength).toInstant.atOffset(ZoneOffset.UTC),
      trafficConsumed = trafficConsumed,
    )
}

object TrafficReportReader {
  def cannotProvide(firstReadable: CantonTimestamp): String =
    s"The sequencer does not keep the intervals before $firstReadable, or they predate the " +
      s"upgrade to it. Request a range that starts at or after $firstReadable."

  def notOver(start: CantonTimestamp, end: CantonTimestamp): String =
    s"The interval [$start, $end) is not over yet, or the sequencer has not sequenced past its " +
      s"end. Request a range that ends at or before $start (or wait for long enough after " +
      s"$end and retry)."
}
