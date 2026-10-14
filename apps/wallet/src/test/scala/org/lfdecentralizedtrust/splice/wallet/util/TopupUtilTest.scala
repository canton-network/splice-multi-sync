// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.wallet.util

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.NonNegativeFiniteDuration

class TopupUtilTest extends BaseTestWordSpec {

  // 2,000 bytes per top-up at $60 per MB is $0.12, or 24 amulets at $0.005 each.
  private val topupParameters =
    ExtraTrafficTopupParameters(2_000L, NonNegativeFiniteDuration.ofMinutes(1))
  private val extraTrafficPrice = BigDecimal(60)
  private val amuletPrice = BigDecimal("0.005")

  "TopupUtil.topupCost" should {

    "price one top-up" in {
      TopupUtil.topupCost(
        topupParameters,
        extraTraffic = 0L,
        extraTrafficPrice,
        amuletPrice,
        registration = None,
      ) shouldBe BigDecimal(24)
    }

    "price a shortfall bought with the top-up as part of it" in {
      // 5,000 bytes in all, so the funds check covers everything the purchase buys.
      TopupUtil.topupCost(
        topupParameters,
        extraTraffic = 3_000L,
        extraTrafficPrice,
        amuletPrice,
        registration = None,
      ) shouldBe BigDecimal(60)
    }
  }
}
