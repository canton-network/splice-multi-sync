// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.ErrorCodeUtils
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import org.lfdecentralizedtrust.splice.automation.{PollingTrigger, TriggerContext}
import org.lfdecentralizedtrust.splice.syncoperator.{SyncOperatorSynchronizerNode, TrafficReport}
import org.lfdecentralizedtrust.splice.syncoperator.store.TrafficReportStore

import scala.concurrent.{ExecutionContext, Future}

/** Stores the traffic charged to participants in each interval of the traffic report once it is
  * over.
  *
  * Reads every participant's cumulative consumed traffic at both ends of the interval from the
  * sequencer's traffic journal. ReconcileDedicatedSynchronizerParametersTrigger keeps the base
  * rate at zero, so extra traffic is all of it, the difference is exactly what the interval
  * charged, and it is the same on every sequencer.
  */
class TrafficReportTrigger(
    override protected val context: TriggerContext,
    synchronizerId: SynchronizerId,
    synchronizerNode: SyncOperatorSynchronizerNode,
    store: TrafficReportStore,
)(implicit
    override val ec: ExecutionContext,
    override val tracer: Tracer,
) extends PollingTrigger {

  override def performWorkIfAvailable()(implicit tc: TraceContext): Future[Boolean] =
    store.lookupLatestIntervalStart(synchronizerId).flatMap { latest =>
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
        synchronizerNode.sequencerAdminConnection.getSequencerPruningStatus().flatMap {
          pruningStatus =>
            val start = next.max(TrafficReport.firstReadableStart(pruningStatus.lowerBound))
            storeInterval(start).map { stored =>
              if (stored && latest.isDefined && start > next)
                logger.warn(
                  s"The sequencer pruned the traffic of the intervals from $next to $start " +
                    "before they were read, so the traffic report cannot provide them"
                )
              stored
            }
        }
    }

  private def storeInterval(start: CantonTimestamp)(implicit tc: TraceContext): Future[Boolean] =
    readTrafficStates(start.plus(TrafficReport.IntervalLength)).flatMap {
      case None => Future.successful(false)
      case Some(atEnd) =>
        readTrafficStates(start).flatMap {
          case None => Future.successful(false)
          case Some(atStart) =>
            store
              .insertInterval(
                synchronizerId,
                start,
                TrafficReport.participantsConsumed(atStart, atEnd),
              )
              .map(_ => true)
        }
    }

  /** Every member's traffic state from just before `time`, or None while the sequencer has not
    * sequenced past `time` yet.
    */
  private def readTrafficStates(
      time: CantonTimestamp
  )(implicit tc: TraceContext): Future[Option[Map[Member, TrafficState]]] =
    synchronizerNode.sequencerAdminConnection
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
            if Option(e.getStatus.getDescription)
              .exists(ErrorCodeUtils.isError(_, SequencerError.NotAtUpgradeTimeOrBeyond)) =>
          None
      }
}
