// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.sv.automation.singlesv.onboarding

import org.lfdecentralizedtrust.splice.automation.{GrantUnlimitedTrafficTriggerBase, TriggerContext}
import org.lfdecentralizedtrust.splice.automation.GrantUnlimitedTrafficTriggerBase.{
  Task,
  UnlimitedTraffic,
}
import org.lfdecentralizedtrust.splice.environment.{
  SequencerAdminConnection,
  SynchronizerNodeService,
}
import org.lfdecentralizedtrust.splice.sv.LocalSynchronizerNode
import org.lfdecentralizedtrust.splice.sv.store.SvDsoStore
import org.lfdecentralizedtrust.splice.util.AmuletConfigSchedule
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

/** This trigger currently relies on enough SVs working on the same set traffic balance request around the same time,
  * as the trigger works with limited parallelism.
  *
  * TODO(tech-debt): remove this constraint by ensuring that we regularly submit set-traffic-balance requests for ALL members.
  */
class SvOnboardingUnlimitedTrafficTrigger(
    override protected val context: TriggerContext,
    dsoStore: SvDsoStore,
    synchronizerNodeService: SynchronizerNodeService[LocalSynchronizerNode],
    trafficBalanceReconciliationDelay: NonNegativeFiniteDuration,
)(implicit
    override val ec: ExecutionContext,
    mat: Materializer,
    override val tracer: Tracer,
) extends GrantUnlimitedTrafficTriggerBase(trafficBalanceReconciliationDelay) {

  override protected def sequencerAdminConnection()(implicit
      tc: TraceContext
  ): Future[SequencerAdminConnection] =
    synchronizerNodeService.sequencerAdminConnection()

  override protected def isActiveMember(task: Task)(implicit
      tc: TraceContext
  ): Future[Boolean] =
    dsoStore
      .getDsoRulesWithSvNodeStates()
      .map(_.activeSvParticipantAndMediatorIds(task.synchronizerId).contains(task.memberId))

  override protected def retrieveTasks()(implicit
      tc: TraceContext
  ): Future[Seq[Task]] = {
    for {
      dsoRulesAndStates <- dsoStore.getDsoRulesWithSvNodeStates()
      amuletRules <- dsoStore.getAmuletRules()
      decentralizedSynchronizerConfig = AmuletConfigSchedule(amuletRules)
        .getConfigAsOf(context.clock.now)
        .decentralizedSynchronizer
      // We assume that we can switch over immediately to the new domain. The one case where this might break
      // is if there is a new SV being onboarded just around the time of the domain migration. This seems
      // like an acceptable limitation.
      activeSynchronizerId = SynchronizerId.tryFromString(
        decentralizedSynchronizerConfig.activeSynchronizer
      )
      connection <- sequencerAdminConnection()
      svMembersWithTrafficState <- MonadUtil
        .sequentialTraverse(
          dsoRulesAndStates
            .activeSvParticipantAndMediatorIds(activeSynchronizerId)
        ) { memberId =>
          for {
            stateO <- connection.lookupSequencerTrafficControlState(memberId)
          } yield {
            if (stateO.isEmpty) {
              // This can happen for mediators which are registered in DsoRules before they connect.
              logger.info(s"Member $memberId does not yet have a traffic state, skipping")
            }
            stateO.map(memberId -> _)
          }
        }
        .map(_.flatten)
    } yield {
      // Sorting here so we have a better chance of all SVs working on the same set traffic balance request around the same time.
      svMembersWithTrafficState.sortBy(_._1).collect {
        case (memberId, trafficState) if trafficState.extraTrafficLimit != UnlimitedTraffic =>
          Task(activeSynchronizerId, memberId)
      }
    }
  }
}
