// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.store

import cats.data.OptionT
import cats.implicits.toBifunctorOps
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.lfdecentralizedtrust.splice.store.KeyValueStore
import org.lfdecentralizedtrust.splice.store.db.StoreDescriptor

import scala.concurrent.{ExecutionContext, Future}

/** The upgrade this operator has scheduled through its admin api. */
final case class ScheduledLsu(
    topologyFreezeTime: CantonTimestamp,
    upgradeTime: CantonTimestamp,
    newPhysicalSynchronizerSerial: NonNegativeInt,
    newPhysicalSynchronizerProtocolVersion: ProtocolVersion,
)

object SyncOperatorKeyValueStore {

  /** The only key this store holds. A second upgrade overwrites the first. */
  val scheduledLsuKey = "scheduled-lsu"

  def apply(
      operatorParty: PartyId,
      participantId: ParticipantId,
      storage: DbStorage,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      lc: ErrorLoggingContext,
      cc: CloseContext,
      tc: TraceContext,
  ): Future[KeyValueStore] =
    KeyValueStore(
      StoreDescriptor(
        version = 1,
        name = "SyncOperatorKeyValueStore",
        party = operatorParty,
        participant = participantId,
        key = Map(
          "operatorParty" -> operatorParty.toProtoPrimitive
        ),
      ),
      storage,
      loggerFactory,
    )

  private implicit val timestampCodec: Codec[CantonTimestamp] =
    Codec
      .from[Long](implicitly, implicitly)
      .iemap(timestamp => CantonTimestamp.fromProtoPrimitive(timestamp).leftMap(_.message))(
        _.toProtoPrimitive
      )

  private implicit val serialCodec: Codec[NonNegativeInt] =
    Codec
      .from[Int](implicitly, implicitly)
      .iemap(NonNegativeInt.create(_).leftMap(_.message))(_.value)

  private implicit val protocolVersionCodec: Codec[ProtocolVersion] =
    Codec.from[String](implicitly, implicitly).iemap(ProtocolVersion.create(_))(_.toString)

  implicit val scheduledLsuCodec: Codec[ScheduledLsu] = deriveCodec[ScheduledLsu]

  def getScheduledLsu(
      store: KeyValueStore
  )(implicit tc: TraceContext, ec: ExecutionContext): OptionT[Future, ScheduledLsu] =
    store.readValueAndLogOnDecodingFailure[ScheduledLsu](scheduledLsuKey)

  def setScheduledLsu(store: KeyValueStore, schedule: ScheduledLsu)(implicit
      tc: TraceContext
  ): Future[Unit] = store.setValue(scheduledLsuKey, schedule)

  def clearScheduledLsu(store: KeyValueStore)(implicit tc: TraceContext): Future[Unit] =
    store.deleteKey(scheduledLsuKey)
}
