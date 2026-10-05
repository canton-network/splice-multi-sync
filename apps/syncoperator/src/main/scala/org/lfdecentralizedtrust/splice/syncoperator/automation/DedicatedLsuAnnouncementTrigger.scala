// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.automation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.lfdecentralizedtrust.splice.automation.TriggerContext
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection
import org.lfdecentralizedtrust.splice.lsu.{LsuAnnouncementTriggerBase, LsuSchedule}
import org.lfdecentralizedtrust.splice.store.KeyValueStore
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorKeyValueStore

import scala.concurrent.{ExecutionContext, Future}

/** Publishes the LSU announcement for the upgrade this operator has scheduled through its admin
  * api.
  *
  * Written through the sequencer, which owns the synchronizer's namespace. The operator's
  * participant holds no key in it.
  */
class DedicatedLsuAnnouncementTrigger(
    override protected val context: TriggerContext,
    override protected val connection: SequencerAdminConnection,
    keyValueStore: KeyValueStore,
)(implicit
    ec: ExecutionContext,
    mat: Materializer,
    tracer: Tracer,
) extends LsuAnnouncementTriggerBase {

  override protected def currentPhysicalSynchronizerId()(implicit
      tc: TraceContext
  ): Future[PhysicalSynchronizerId] = connection.getPhysicalSynchronizerId()

  override protected def upgradeDueAt(now: CantonTimestamp)(implicit
      tc: TraceContext
  ): Future[Option[LsuSchedule]] =
    SyncOperatorKeyValueStore
      .getScheduledLsu(keyValueStore)
      .filter(schedule => !now.isBefore(schedule.topologyFreezeTime))
      .map(schedule =>
        LsuSchedule(
          upgradeTime = schedule.upgradeTime,
          successorSerial = schedule.newPhysicalSynchronizerSerial,
          successorProtocolVersion = schedule.newPhysicalSynchronizerProtocolVersion,
        )
      )
      .value
}
