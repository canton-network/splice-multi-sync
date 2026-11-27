// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, SyncCloseable}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.synchronizer.sequencer.{
  SequencerMemberStatus,
  SequencerPruningStatus,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import io.grpc.Status
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection

import scala.concurrent.Future
import scala.jdk.DurationConverters.*

/** Prunes a sequencer up to `retentionPeriod` before now, first disabling the members that have
  * not acknowledged that far.
  *
  * Subclasses supply the sequencer, which of its members the app runs, and when it can be pruned.
  */
abstract class SequencerPruningTriggerBase(retentionPeriod: NonNegativeFiniteDuration)
    extends PollingTrigger {

  protected def sequencerToPrune()(implicit tc: TraceContext): Future[SequencerAdminConnection]

  /** Those of `members` that this app runs. Pruning fails rather than disabling them. */
  protected def ownMembers(members: Seq[Member])(implicit tc: TraceContext): Future[Seq[Member]]

  protected def readyToPrune(sequencer: SequencerAdminConnection)(implicit
      tc: TraceContext
  ): Future[Boolean]

  private val pruningMetrics = new SequencerPruningMetrics(context.metricsFactory)

  override def performWorkIfAvailable()(implicit traceContext: TraceContext): Future[Boolean] =
    for {
      sequencer <- sequencerToPrune()
      ready <- readyToPrune(sequencer)
      _ <-
        if (ready) {
          logger.info("Attempt pruning our sequencer...")
          prune(sequencer).map { prunedResult =>
            logger.info(s"Completed pruning our sequencer with result: $prunedResult")
          }
        } else Future.unit
    } yield false

  // This method is replicating the force_prune command defined in `com.digitalasset.canton.console.commands.SequencerAdministrationGroupCommon`
  // Which will prunes the member preventing pruning
  private def prune(sequencer: SequencerAdminConnection)(implicit traceContext: TraceContext) =
    for {
      status <- sequencer.getSequencerPruningStatus()
      pruningTimestamp = status.now.minus(retentionPeriod.underlying.toJava)
      membersToDisable = clientsPreventingPruning(status, pruningTimestamp)
      _ <-
        // disabling member preventing pruning
        if (membersToDisable.nonEmpty) {
          ownMembers(membersToDisable.map(_.member)).flatMap { own =>
            val ourLaggingMembers = membersToDisable.filter(m => own.contains(m.member))
            if (ourLaggingMembers.isEmpty) {
              logger.info(
                show"disabling ${membersToDisable.size} member clients preventing pruning to $pruningTimestamp: $membersToDisable"
              )
              pruningMetrics.disabledMembers.updateValue(membersToDisable.size)
              Future.traverse(membersToDisable)(m => sequencer.disableMember(m.member))
            } else {
              throw Status.INTERNAL
                .withDescription(
                  show"Failed to prune sequencer to $pruningTimestamp because our own nodes have not acknowledged that timestamp: ${ourLaggingMembers}"
                )
                .asRuntimeException()
            }
          }
        } else Future.unit
      statusAfterDisabling <- sequencer.getSequencerPruningStatus()
      safeTimestamp = statusAfterDisabling.safePruningTimestamp
      res <-
        if (safeTimestamp < pruningTimestamp) {
          val message = (
            s"We disabled all clients preventing pruning at $pruningTimestamp however the safe timestamp is set to $safeTimestamp"
          )
          Future.failed(Status.INTERNAL.withDescription(message).asRuntimeException())
        } else
          pruningMetrics.latency
            .timeFuture(
              sequencer
                .prune(pruningTimestamp)
            )
            .transform(
              identity,
              err => {
                val lastAcknowledged =
                  statusAfterDisabling.members
                    .collect { case m: SequencerMemberStatus.Active =>
                      m.member.toProtoPrimitive -> m.safePruningTimestamp
                    }
                val message = s"failed to prune with sequencer pruning status: $lastAcknowledged"
                if (context.retryProvider.isClosing)
                  logger.info(message)
                else
                  logger.warn(message)
                err
              },
            )

    } yield res

  private def clientsPreventingPruning(
      status: SequencerPruningStatus,
      timestamp: CantonTimestamp,
  ): Seq[SequencerPruningTriggerBase.LaggingMember] = {
    val memberToSafePruningTimestamp: Map[Member, CantonTimestamp] =
      status.members.view.collect { case m: SequencerMemberStatus.Active =>
        m.member -> m.safePruningTimestamp
      }.toMap
    status
      .clientsPreventingPruning(timestamp)
      .members
      .toList
      .map(m => SequencerPruningTriggerBase.LaggingMember(m, memberToSafePruningTimestamp(m)))
  }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] =
    SyncCloseable("Pruning Metrics", pruningMetrics.close()) +: super.closeAsync()
}

private object SequencerPruningTriggerBase {
  final case class LaggingMember(
      member: Member,
      safePruningTimestamp: CantonTimestamp,
  ) extends PrettyPrinting {
    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("member", _.member),
        param("safePruningTimestamp", _.safePruningTimestamp),
      )
  }
}
