// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.{Member, ParticipantId}
import org.lfdecentralizedtrust.splice.http.v0.definitions.TrafficReportInterval
import org.lfdecentralizedtrust.splice.http.v0.definitions.TrafficReportInterval.Status
import org.lfdecentralizedtrust.splice.store.Limit

import java.time.{Duration, ZoneOffset}

/** The intervals of the traffic report: 10 minutes of record time each, aligned in UTC at XX:00,
  * XX:10, and so on.
  */
object TrafficReport {

  val IntervalLength: Duration = Duration.ofMinutes(10)

  private val intervalMicros: Long = IntervalLength.toNanos / 1000

  def alignDown(time: CantonTimestamp): CantonTimestamp =
    CantonTimestamp.assertFromLong(time.toMicros - Math.floorMod(time.toMicros, intervalMicros))

  /** The first interval that starts after the sequencer's pruning lower bound. Pruning keeps only
    * each member's latest journal row up to that bound, so readings at or before it are wrong.
    * Canton prunes the journal before it moves the bound, so an app that catches up at the edge of
    * the retention period can still race a prune and store a wrong total.
    */
  def firstReadableStart(pruningLowerBound: CantonTimestamp): CantonTimestamp =
    alignDown(pruningLowerBound).plus(IntervalLength)

  /** The starts of the intervals in `[start, end)`, if both are aligned and the range has at most
    * one page of intervals.
    */
  def intervalStarts(
      start: CantonTimestamp,
      end: CantonTimestamp,
  ): Either[String, Seq[CantonTimestamp]] =
    if (alignDown(start) != start || alignDown(end) != end)
      Left(s"Start and end must be aligned to 10-minute boundaries, but got [$start, $end)")
    else if (start >= end) Left(s"Start must be before end, but got [$start, $end)")
    else {
      val count = (end.toMicros - start.toMicros) / intervalMicros
      if (count > Limit.DefaultMaxPageSize)
        Left(
          s"[$start, $end) has $count intervals, at most ${Limit.DefaultMaxPageSize} are allowed"
        )
      else Right((0L until count).map(i => start.plus(IntervalLength.multipliedBy(i))))
    }

  /** The traffic charged to participants between two readings of the sequencer's traffic states.
    * A participant without a reading at the start joined since then and counts from zero. One
    * without a reading at the end left since then and is not counted.
    */
  def participantsConsumed(
      atStart: Map[Member, TrafficState],
      atEnd: Map[Member, TrafficState],
  ): Long =
    atEnd.iterator.collect { case (participant: ParticipantId, state) =>
      state.extraTrafficConsumed.value -
        atStart.get(participant).fold(0L)(_.extraTrafficConsumed.value)
    }.sum

  /** Reports each interval as `Ok` with its stored total. An interval with no stored total is
    * `CannotProvide` if a later one is stored, because intervals are stored in order and one that
    * was skipped is never stored, and `Undetermined` otherwise.
    */
  def report(
      starts: Seq[CantonTimestamp],
      stored: Map[CantonTimestamp, Long],
      latestStored: Option[CantonTimestamp],
  ): Vector[TrafficReportInterval] =
    starts.map { start =>
      val startRecordTime = start.toInstant.atOffset(ZoneOffset.UTC)
      stored.get(start) match {
        case Some(trafficConsumed) =>
          TrafficReportInterval(startRecordTime, Status.Ok, Some(trafficConsumed))
        case None if latestStored.exists(_ > start) =>
          TrafficReportInterval(startRecordTime, Status.CannotProvide)
        case None => TrafficReportInterval(startRecordTime, Status.Undetermined)
      }
    }.toVector
}
