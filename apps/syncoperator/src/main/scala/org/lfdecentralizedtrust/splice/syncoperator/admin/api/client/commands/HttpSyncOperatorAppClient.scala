// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.admin.api.client.commands

import com.digitalasset.canton.data.CantonTimestamp
import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.lfdecentralizedtrust.splice.admin.api.client.commands.HttpCommand
import org.lfdecentralizedtrust.splice.http.v0.{definitions, sync_operator as http}
import org.lfdecentralizedtrust.splice.util.TemplateJsonDecoder

import java.time.ZoneOffset

object HttpSyncOperatorAppClient {
  import http.SyncOperatorClient as Client

  abstract class BaseCommand[Res, Result] extends HttpCommand[Res, Result, Client] {
    override val createGenClientFn = (fn, host, ec, mat) => Client.httpClient(fn, host)(ec, mat)
  }

  case class GetTrafficReport(start: CantonTimestamp, end: CantonTimestamp)
      extends BaseCommand[
        http.GetTrafficReportResponse,
        Seq[definitions.TrafficReportInterval],
      ] {

    override def submitRequest(client: Client, headers: List[HttpHeader]) =
      client.getTrafficReport(
        start.toInstant.atOffset(ZoneOffset.UTC),
        end.toInstant.atOffset(ZoneOffset.UTC),
        headers,
      )

    override def handleOk()(implicit decoder: TemplateJsonDecoder) = {
      case http.GetTrafficReportResponse.OK(response) => Right(response.intervals)
    }
  }
}
