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
import scala.util.control.NonFatal

/** Grants the traffic purchased via `MemberTraffic` contracts on the sequencer of a single
  * synchronizer.
  *
  * Traffic can be purchased for any synchronizer, so a node observes `MemberTraffic` contracts
  * naming synchronizers it does not serve. Each instance of this trigger owns exactly one
  * synchronizer, given by [[targetSynchronizerId]], and skips everything else; the operator of
  * another synchronizer grants those purchases on its own sequencer.
  *
  * One instance reconciles against one sequencer, which is the MVP shape. A BFT synchronizer has
  * several, and a traffic grant is aggregated across the sequencer group and only commits once its
  * threshold is met, so its operator runs one instance of this trigger per sequencer, each with its
  * own [[sequencerAdminConnection]]. Nothing here assumes the single-sequencer case beyond that
  * hook returning one connection.
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

  /** Set once the mismatch has been logged, so that it is reported once rather than per task. */
  private val targetMismatchWarned = new AtomicBoolean(false)

  /** Set once the sequencer has been seen serving [[targetSynchronizerId]], so that the skip path
    * stops looking it up. Deliberately separate from [[targetMismatchWarned]]: confirming the
    * wiring must not consume the one warning a later mismatch is entitled to.
    */
  private val targetConfirmed = new AtomicBoolean(false)

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
                  val outcome = TaskSuccess(
                    s"Skipping MemberTraffic contract for synchronizer $synchronizerId, " +
                      s"this trigger reconciles $targetSynchronizerId"
                  )
                  // A misconfigured target makes every contract land here, which would otherwise
                  // leave the trigger silently granting nothing. The check is best-effort so that
                  // skipping never depends on the sequencer being reachable.
                  warnOnceIfTargetNotServed().map(_ => outcome)
                } else {
                  processMember(memberId)
                },
            ),
      )
  }

  private def processMember(memberId: Member)(implicit tc: TraceContext): Future[TaskOutcome] =
    trafficLimitOffset(memberId).flatMap {
      case Left(reason) =>
        Future.successful(TaskSuccess(s"Skipping MemberTraffic contract for $memberId: $reason"))
      case Right(offset) =>
        servedSynchronizerId().flatMap {
          case (_, None) =>
            Future.failed(
              Status.FAILED_PRECONDITION
                .withDescription("Sequencer is not yet initialized")
                .asRuntimeException()
            )
          case (_, Some(served)) if served != targetSynchronizerId =>
            // Granting here would credit a sequencer of a different synchronizer. Reported as
            // retryable so that a connection that is still switching over recovers on its own
            // instead of dropping the purchase. A permanent miswiring therefore retries rather
            // than failing once, so warn alongside it to give the retries a one-line diagnosis.
            warnOnceTargetNotServed(served)
            Future.failed(
              Status.FAILED_PRECONDITION
                .withDescription(
                  s"The sequencer admin connection serves $served, " +
                    s"but this trigger reconciles $targetSynchronizerId"
                )
                .asRuntimeException()
            )
          case (sequencerAdminConnection, _) =>
            reconcileExtraTrafficLimitForMember(memberId, offset, sequencerAdminConnection)
        }
    }

  /** The connection we would grant on, together with the synchronizer it serves. The synchronizer
    * is absent while the sequencer is still initializing.
    */
  private def servedSynchronizerId()(implicit
      tc: TraceContext
  ): Future[(SequencerAdminConnection, Option[SynchronizerId])] =
    sequencerAdminConnection().flatMap { connection =>
      connection.getStatus.map { status =>
        (connection, status.successOption.map(_.synchronizerId.logical))
      }
    }

  /** Logs at most once that the sequencer we would grant on serves `served` rather than
    * [[targetSynchronizerId]].
    */
  private def warnOnceTargetNotServed(served: SynchronizerId)(implicit tc: TraceContext): Unit =
    if (targetMismatchWarned.compareAndSet(false, true)) {
      logger.warn(
        s"This trigger reconciles $targetSynchronizerId, but its sequencer serves " +
          s"$served, so no traffic is granted while that remains the case"
      )
    }

  /** Looks the sequencer up to warn on a mismatch, for callers that do not already know what it
    * serves. Best-effort: an unreachable or uninitialized sequencer leaves the check for a later
    * task and never fails the caller.
    */
  private def warnOnceIfTargetNotServed()(implicit tc: TraceContext): Future[Unit] =
    if (targetConfirmed.get() || targetMismatchWarned.get()) {
      Future.unit
    } else {
      // delegate so that a subclass throwing instead of failing its future is still caught below
      Future
        .delegate(servedSynchronizerId())
        .map {
          case (_, Some(served)) if served != targetSynchronizerId =>
            warnOnceTargetNotServed(served)
          case (_, Some(_)) =>
            targetConfirmed.set(true)
          case (_, None) =>
            // Inconclusive, so leave the check for a later task.
            ()
        }
        .recover { case NonFatal(e) =>
          logger.debug(s"Could not check the sequencer of $targetSynchronizerId: $e")
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
