// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.automation

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.codegen.java.splice
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.environment.TopologyAdminConnection.TopologySnapshot
import org.lfdecentralizedtrust.splice.store.AppStore
import org.lfdecentralizedtrust.splice.util.AssignedContract

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

/** Grants the traffic purchased via `MemberTraffic` contracts on the sequencer of a single
  * synchronizer.
  *
  * Traffic can be purchased for any synchronizer, so a node observes `MemberTraffic` contracts
  * naming synchronizers it does not serve. Each instance of this trigger owns exactly one
  * synchronizer, given by [[targetSynchronizerId]], and skips everything else; the operator of
  * another synchronizer grants those purchases on its own sequencer.
  *
  * [[targetSynchronizerId]] is a stable value for the lifetime of the trigger, so a node that
  * changes the logical synchronizer it serves has to be restarted. Note that the sibling
  * `SvOnboardingUnlimitedTrafficTrigger` instead re-resolves the active synchronizer from
  * `AmuletConfigSchedule` on every run, so the two source the same concept differently.
  *
  * Purchases are matched on the synchronizer id alone. Whether a purchase recorded against a
  * different migration id of the same synchronizer is visible at all is decided upstream, by the
  * ingestion filter of the store backing this trigger.
  *
  * This trigger currently relies on enough nodes working on the same set traffic balance request
  * around the same time. It also depends on the sorting of tasks done in OnAssignedContractTrigger
  * to make this more likely to succeed.
  *
  * TODO(tech-debt): remove this constraint by ensuring that we regularly submit set-traffic-balance
  * requests for ALL members.
  */
abstract class ReconcileSequencerLimitWithMemberTrafficTriggerBase(
    store: AppStore,
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    ec: ExecutionContext,
    mat: Materializer,
    tracer: Tracer,
) extends OnAssignedContractTrigger.Template[
      splice.decentralizedsynchronizer.MemberTraffic.ContractId,
      splice.decentralizedsynchronizer.MemberTraffic,
    ](
      store,
      splice.decentralizedsynchronizer.MemberTraffic.COMPANION,
    ) {

  /** The synchronizer this trigger reconciles. `MemberTraffic` contracts for any other
    * synchronizer are skipped.
    */
  protected def targetSynchronizerId: SynchronizerId

  /** Admin connection to the sequencer serving [[targetSynchronizerId]]. */
  protected def sequencerAdminConnection()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection]

  /** Total traffic purchased for `memberId` on [[targetSynchronizerId]]. */
  protected def getTotalPurchasedMemberTraffic(memberId: Member)(implicit
      tc: TraceContext
  ): Future[Long]

  /** The traffic already consumed by `memberId` before this trigger took over its reconciliation,
    * added to the purchased total when setting the limit. A `Left` skips the member and carries the
    * reason, which is how members that are granted unlimited traffic are excluded.
    */
  protected def trafficLimitOffset(memberId: Member)(implicit
      tc: TraceContext
  ): Future[Either[String, Long]]

  /** Guards the one-off check in [[warnOnceIfTargetNotServed]]. */
  private val targetChecked = new AtomicBoolean(false)

  override def completeTask(
      memberTraffic: AssignedContract[
        splice.decentralizedsynchronizer.MemberTraffic.ContractId,
        splice.decentralizedsynchronizer.MemberTraffic,
      ]
  )(implicit tc: TraceContext): Future[TaskOutcome] = {
    Member
      .fromProtoPrimitive_(memberTraffic.payload.memberId)
      .fold(
        err =>
          // Skip contracts with invalid member ids
          Future.successful(TaskSuccess(s"Skipping MemberTraffic with invalid memberId: ${err}")),
        memberId =>
          SynchronizerId
            .fromString(memberTraffic.payload.synchronizerId)
            .fold(
              err =>
                // Skip contracts with invalid synchronizer ids
                Future.successful(
                  TaskSuccess(s"Skipping MemberTraffic with invalid synchronizerId: ${err}")
                ),
              synchronizerId =>
                if (synchronizerId != targetSynchronizerId) {
                  // A misconfigured target makes every contract land here, which would otherwise
                  // leave the trigger silently granting nothing, so check the target itself once.
                  warnOnceIfTargetNotServed().map { _ =>
                    TaskSuccess(
                      s"Skipping MemberTraffic contract for synchronizer $synchronizerId, " +
                        s"this trigger reconciles $targetSynchronizerId"
                    )
                  }
                } else {
                  reconcileMember(memberId)
                },
            ),
      )
  }

  private def reconcileMember(memberId: Member)(implicit tc: TraceContext): Future[TaskOutcome] =
    trafficLimitOffset(memberId).flatMap {
      case Left(reason) =>
        Future.successful(TaskSuccess(s"Skipping MemberTraffic contract for $memberId: $reason"))
      case Right(offset) =>
        sequencerAdminConnection().flatMap { sequencerAdminConnection =>
          sequencerAdminConnection.getStatus
            .map(_.successOption.map(_.synchronizerId))
            .flatMap {
              case None =>
                Future.failed(
                  Status.FAILED_PRECONDITION
                    .withDescription("Sequencer is not yet initialized")
                    .asRuntimeException()
                )
              case Some(sequencerSynchronizerId)
                  if sequencerSynchronizerId.logical != targetSynchronizerId =>
                // Granting here would credit a sequencer of a different synchronizer. Reported as
                // retryable so that a connection that is still switching over recovers on its own
                // instead of dropping the purchase.
                Future.failed(
                  Status.FAILED_PRECONDITION
                    .withDescription(
                      s"The sequencer admin connection serves ${sequencerSynchronizerId.logical}, " +
                        s"but this trigger reconciles $targetSynchronizerId"
                    )
                    .asRuntimeException()
                )
              case _ =>
                reconcileExtraTrafficLimitForMember(memberId, offset, sequencerAdminConnection)
            }
        }
    }

  /** Logs at most once if the sequencer we would grant on does not serve [[targetSynchronizerId]].
    * A sequencer that is not initialized yet leaves the check pending for a later task.
    */
  private def warnOnceIfTargetNotServed()(implicit tc: TraceContext): Future[Unit] =
    if (targetChecked.get()) {
      Future.unit
    } else {
      sequencerAdminConnection()
        .flatMap(_.getStatus)
        .map(_.successOption.map(_.synchronizerId))
        .map {
          case Some(sequencerSynchronizerId) =>
            if (sequencerSynchronizerId.logical != targetSynchronizerId) {
              logger.warn(
                s"This trigger reconciles $targetSynchronizerId, but its sequencer serves " +
                  s"${sequencerSynchronizerId.logical}, so no traffic will ever be granted"
              )
            }
            targetChecked.set(true)
          case None => ()
        }
    }

  private def reconcileExtraTrafficLimitForMember(
      memberId: Member,
      offset: Long,
      sequencerAdminConnection: SequencerAdminConnection,
  )(implicit tc: TraceContext): Future[TaskSuccess] = {
    sequencerAdminConnection.lookupSequencerTrafficControlState(memberId).flatMap {
      case None =>
        Future.successful(
          TaskSuccess(
            s"No traffic state found for member $memberId. It is likely that the member has been disabled as it was lagging behind and prevented sequencer pruning."
          )
        )
      case Some(trafficState) =>
        for {
          // Compute new extra traffic limit
          totalPurchasedTraffic <- getTotalPurchasedMemberTraffic(memberId)
          newExtraTrafficLimit = NonNegativeLong
            .tryCreate(offset + totalPurchasedTraffic)

          // Get current effective sequencer domain state
          sequencerSynchronizerState <- sequencerAdminConnection
            .getSequencerSynchronizerState(topologySnapshot = TopologySnapshot.Effective)
          currentExtraTrafficLimit = trafficState.extraTrafficLimit

          // Compare and reconcile old and new limits
          taskOutcome <-
            if (currentExtraTrafficLimit < newExtraTrafficLimit) {
              sequencerAdminConnection
                .setSequencerTrafficControlState(
                  trafficState,
                  sequencerSynchronizerState,
                  newExtraTrafficLimit,
                  context.pollingClock,
                  trafficBalanceReconciliationDelay,
                )
                .map(_ =>
                  TaskSuccess(
                    s"Updated extra traffic limit for member ${memberId} from ${currentExtraTrafficLimit} to ${newExtraTrafficLimit}"
                  )
                )
            } else {
              Future.successful(
                TaskSuccess(
                  s"Skipping since traffic limit is already up to date (previous limit = ${currentExtraTrafficLimit}, new limit = ${newExtraTrafficLimit})."
                )
              )
            }
        } yield taskOutcome
    }
  }
}
