// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.admin.api.client.commands

import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.lfdecentralizedtrust.splice.admin.api.client.commands.HttpCommand
import org.lfdecentralizedtrust.splice.http.v0.definitions
import org.lfdecentralizedtrust.splice.http.v0.external.sync_operator as http
import org.lfdecentralizedtrust.splice.util.TemplateJsonDecoder

object HttpSyncOperatorAppClient {
  import http.SyncOperatorClient as Client

  abstract class BaseCommand[Res, Result] extends HttpCommand[Res, Result, Client] {
    override val createGenClientFn = (fn, host, ec, mat) => Client.httpClient(fn, host)(ec, mat)
  }

  case object CheckAuth extends BaseCommand[http.CheckAuthResponse, definitions.CheckAuthResponse] {

    override def submitRequest(client: Client, headers: List[HttpHeader]) =
      client.checkAuth(headers)

    override def handleOk()(implicit decoder: TemplateJsonDecoder) = {
      case http.CheckAuthResponse.OK(response) => Right(response)
    }
  }
}
