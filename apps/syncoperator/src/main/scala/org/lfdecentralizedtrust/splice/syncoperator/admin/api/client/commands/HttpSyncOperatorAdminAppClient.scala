// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.admin.api.client.commands

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.http.scaladsl.model.{HttpHeader, HttpResponse}
import org.lfdecentralizedtrust.splice.admin.api.client.commands.HttpCommand
import org.lfdecentralizedtrust.splice.http.v0.definitions
import org.lfdecentralizedtrust.splice.http.v0.sync_operator_admin as http
import org.lfdecentralizedtrust.splice.util.TemplateJsonDecoder

import java.time.ZoneOffset
import scala.concurrent.Future

object HttpSyncOperatorAdminAppClient {
  import http.SyncOperatorAdminClient as Client

  abstract class BaseCommand[Res, Result] extends HttpCommand[Res, Result, Client] {
    override val createGenClientFn = (fn, host, ec, mat) => Client.httpClient(fn, host)(ec, mat)
  }

  case class ScheduleLogicalSynchronizerUpgrade(
      topologyFreezeTime: CantonTimestamp,
      upgradeTime: CantonTimestamp,
      newPhysicalSynchronizerSerial: NonNegativeInt,
      newPhysicalSynchronizerProtocolVersion: ProtocolVersion,
  ) extends BaseCommand[http.ScheduleLogicalSynchronizerUpgradeResponse, Unit] {

    override def submitRequest(
        client: Client,
        headers: List[HttpHeader],
    ): EitherT[Future, Either[
      Throwable,
      HttpResponse,
    ], http.ScheduleLogicalSynchronizerUpgradeResponse] =
      client.scheduleLogicalSynchronizerUpgrade(
        definitions.ScheduleLogicalSynchronizerUpgradeRequest(
          topologyFreezeTime = topologyFreezeTime.toInstant.atOffset(ZoneOffset.UTC),
          upgradeTime = upgradeTime.toInstant.atOffset(ZoneOffset.UTC),
          newPhysicalSynchronizerSerial = newPhysicalSynchronizerSerial.value,
          newPhysicalSynchronizerProtocolVersion =
            newPhysicalSynchronizerProtocolVersion.toString,
        ),
        headers = headers,
      )

    override def handleOk()(implicit
        decoder: TemplateJsonDecoder
    ) = { case http.ScheduleLogicalSynchronizerUpgradeResponse.OK =>
      Right(())
    }
  }

  case class CancelLogicalSynchronizerUpgrade()
      extends BaseCommand[http.CancelLogicalSynchronizerUpgradeResponse, Unit] {

    override def submitRequest(
        client: Client,
        headers: List[HttpHeader],
    ): EitherT[Future, Either[
      Throwable,
      HttpResponse,
    ], http.CancelLogicalSynchronizerUpgradeResponse] =
      client.cancelLogicalSynchronizerUpgrade(headers = headers)

    override def handleOk()(implicit
        decoder: TemplateJsonDecoder
    ) = { case http.CancelLogicalSynchronizerUpgradeResponse.OK =>
      Right(())
    }
  }
}
