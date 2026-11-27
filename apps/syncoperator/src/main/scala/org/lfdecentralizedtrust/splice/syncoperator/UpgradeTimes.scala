// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import org.lfdecentralizedtrust.splice.environment.SequencerAdminConnection

import scala.concurrent.{ExecutionContext, Future}

/** The upgrades that made a sequencer current and that move to its successor, if any. */
final case class UpgradeTimes(
    currentFrom: Option[CantonTimestamp],
    successorFrom: Option[CantonTimestamp],
)

object UpgradeTimes {

  /** A sequencer's topology still lists the upgrade that made it current. */
  def of(sequencer: SequencerAdminConnection, synchronizerId: SynchronizerId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[UpgradeTimes] =
    for {
      psid <- sequencer.getPhysicalSynchronizerId()
      announcements <- sequencer.listLsuAnnouncements(synchronizerId)
    } yield {
      val upgrades = announcements.map(_.mapping)
      UpgradeTimes(
        currentFrom =
          upgrades.find(_.successorSynchronizerId.serial == psid.serial).map(_.upgradeTime),
        successorFrom =
          upgrades.find(_.successorSynchronizerId.serial > psid.serial).map(_.upgradeTime),
      )
    }
}
