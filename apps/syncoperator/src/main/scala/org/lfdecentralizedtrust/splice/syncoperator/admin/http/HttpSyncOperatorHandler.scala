// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.syncoperator.admin.http

import org.lfdecentralizedtrust.splice.auth.AuthenticationOnlyAuthExtractor.AuthenticatedRequest
import org.lfdecentralizedtrust.splice.http.v0.definitions
import org.lfdecentralizedtrust.splice.http.v0.external.sync_operator as v0

import scala.concurrent.Future

class HttpSyncOperatorHandler extends v0.SyncOperatorHandler[AuthenticatedRequest] {

  override def checkAuth(
      respond: v0.SyncOperatorResource.CheckAuthResponse.type
  )()(extracted: AuthenticatedRequest): Future[v0.SyncOperatorResource.CheckAuthResponse] =
    Future.successful(respond.OK(definitions.CheckAuthResponse(extracted.user)))
}
