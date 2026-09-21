package org.lfdecentralizedtrust.splice.util

import org.lfdecentralizedtrust.splice.codegen.java.splice.amulet.Amulet
import org.lfdecentralizedtrust.splice.codegen.java.splice.fees.{ExpiringAmount, RatePerRound}
import org.lfdecentralizedtrust.splice.codegen.java.splice.types.Round
import com.digitalasset.canton.BaseTest
import java.math.BigDecimal
import org.scalatest.wordspec.AnyWordSpec

class SpliceUtilTest extends AnyWordSpec with BaseTest {
  val amulet = new Amulet(
    "dso",
    "dso",
    new ExpiringAmount(
      new BigDecimal(1.0).setScale(10),
      new Round(0L),
      new RatePerRound(new BigDecimal(0.5).setScale(10)),
    ),
  )
  "compute holding fees" in {
    SpliceUtil.holdingFee(amulet, 0L) shouldBe new BigDecimal(0.0).setScale(10)
    SpliceUtil.holdingFee(amulet, 1L) shouldBe new BigDecimal(0.5).setScale(10)
    SpliceUtil.holdingFee(amulet, 2L) shouldBe new BigDecimal(1.0).setScale(10)
    // Capped at initial amount
    SpliceUtil.holdingFee(amulet, 3L) shouldBe new BigDecimal(1.0).setScale(10)
  }

  "compute synchronizer fees" should {
    // 2.5MB at $1.00/MB, the same inputs as `test_BuyRegisteredSyncTraffic_discounted`.
    val topupAmount = 2_500_000L
    val extraTrafficPrice = scala.math.BigDecimal(1)
    val amuletPrice = scala.math.BigDecimal("0.005")

    "price an undiscounted purchase as before" in {
      SpliceUtil.synchronizerFees(topupAmount, extraTrafficPrice, amuletPrice) shouldBe
        (scala.math.BigDecimal("2.5"), scala.math.BigDecimal(500))
    }

    "apply the discount, agreeing with what the buy charges on-ledger" in {
      val (usd, cc) =
        SpliceUtil.synchronizerFees(
          topupAmount,
          extraTrafficPrice,
          amuletPrice,
          scala.math.BigDecimal("0.75"),
        )
      usd shouldBe scala.math.BigDecimal("1.875")
      cc shouldBe scala.math.BigDecimal(375)
    }
  }
}
