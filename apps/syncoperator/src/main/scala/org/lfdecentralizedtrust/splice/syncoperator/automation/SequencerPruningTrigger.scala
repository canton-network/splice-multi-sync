// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{Member, ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.lfdecentralizedtrust.splice.automation.{SequencerPruningTriggerBase, TriggerContext}
import org.lfdecentralizedtrust.splice.environment.{
  SequencerAdminConnection,
  SynchronizerNodeService,
}
import org.lfdecentralizedtrust.splice.syncoperator.{SyncOperatorSynchronizerNode, UpgradeTimes}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.DurationConverters.*

/** Prunes the sequencer this operator runs, disabling the participants that hold pruning back. */
class SequencerPruningTrigger(
    override protected val context: TriggerContext,
    synchronizerNodeService: SynchronizerNodeService[SyncOperatorSynchronizerNode],
    synchronizerId: SynchronizerId,
    retentionPeriod: NonNegativeFiniteDuration,
)(implicit
    override val ec: ExecutionContext,
    override val tracer: Tracer,
) extends SequencerPruningTriggerBase(retentionPeriod) {

  override protected def sequencerToPrune()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection] = synchronizerNodeService.sequencerAdminConnection()

  /** The operator runs every node but the participants. */
  override protected def ownMembers(members: Seq[Member])(implicit
      tc: TraceContext
  ): Future[Seq[Member]] =
    Future.successful(members.filter {
      case _: ParticipantId => false
      case _ => true
    })

  // Waits until a successor has served for the retention period, like the SV app's trigger, so it
  // doesn't disable members that have not acknowledged on it yet.
  // TODO(DACH-NY/cn-test-failures#8065) Remove once Canton transfers safe pruning timestamps on LSU.
  override protected def readyToPrune(sequencer: SequencerAdminConnection)(implicit
      tc: TraceContext
  ): Future[Boolean] =
    for {
      status <- sequencer.getSequencerPruningStatus()
      upgrades <- UpgradeTimes.of(sequencer, synchronizerId)
    } yield {
      val ready =
        upgrades.currentFrom.forall(_ < status.now.minus(retentionPeriod.underlying.toJava))
      if (!ready)
        logger.debug(
          s"The sequencer has served for less than $retentionPeriod since the upgrade at ${upgrades.currentFrom}"
        )
      ready
    }
}
