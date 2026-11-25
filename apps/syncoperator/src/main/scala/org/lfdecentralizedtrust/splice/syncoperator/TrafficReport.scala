// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.{Member, ParticipantId}

import java.time.Duration

/** The intervals of the traffic report, aligned at multiples of their length since the Unix epoch.
  * At the default length of 10 minutes, they start at XX:00, XX:10, and so on in UTC.
  */
final case class TrafficReport(intervalLength: Duration) {
  import TrafficReport.*

  private val intervalMicros: Long = intervalLength.toNanos / 1000

  def alignDown(time: CantonTimestamp): CantonTimestamp =
    CantonTimestamp.assertFromLong(time.toMicros - Math.floorMod(time.toMicros, intervalMicros))

  /** The first interval that starts after the sequencer's pruning lower bound. Pruning keeps only
    * each member's latest journal row up to that bound, so readings at or before it are wrong.
    */
  def firstReadableStart(pruningLowerBound: CantonTimestamp): CantonTimestamp =
    alignDown(pruningLowerBound).plus(intervalLength)

  /** The first page of the intervals in `[start, end)`, if both are aligned. */
  def page(start: CantonTimestamp, end: CantonTimestamp): Either[String, Page] =
    if (alignDown(start) != start || alignDown(end) != end)
      Left(s"Start and end must be aligned to intervals of $intervalLength, but got [$start, $end)")
    else if (start >= end) Left(s"Start must be before end, but got [$start, $end)")
    else {
      val starts =
        Iterator.iterate(start)(_.plus(intervalLength)).takeWhile(_ < end).take(PageSize).toSeq
      Right(Page(starts, starts.lastOption.map(_.plus(intervalLength)).filter(_ < end)))
    }
}

object TrafficReport {

  /** Each interval takes a reading from the sequencer, so a response covers at most this many. */
  val PageSize: Int = 100

  /** The traffic charged to participants between two readings of the sequencer's traffic states.
    * A participant without a reading at the start joined since then and counts from zero. One
    * without a reading at the end left since then and isn't counted: the sequencer no longer
    * lists it, so its traffic up to when it left can't be read.
    */
  def participantsConsumed(
      atStart: Map[Member, TrafficState],
      atEnd: Map[Member, TrafficState],
  ): Long =
    atEnd.iterator.collect { case (participant: ParticipantId, state) =>
      state.extraTrafficConsumed.value -
        atStart.get(participant).fold(0L)(_.extraTrafficConsumed.value)
    }.sum

  /** The starts of a page of intervals, and where the next page starts if the range goes on. */
  final case class Page(starts: Seq[CantonTimestamp], nextStart: Option[CantonTimestamp])
}
