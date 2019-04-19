package com.wavesplatform.settings

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class UTXSettingsSpecification extends FlatSpec with Matchers {
  "UTXSettings" should "read values" in {
    val config   = ConfigFactory.parseString("""TN {
        |  utx {
        |    max-size = 100
        |    max-transaction-age = 100m
        |    cleanup-interval = 10m
        |    blacklist-sender-addresses = ["a"]
        |    allow-blacklisted-transfer-to = ["b"]
        |  }
        |}""".stripMargin).resolve()
    val settings = config.as[UtxSettings]("TN.utx")
    settings.maxSize should be(100)
    settings.maxTransactionAge shouldBe 100.minutes
    settings.cleanupInterval shouldBe 10.minutes
    settings.blacklistSenderAddresses shouldBe Set("a")
    settings.allowBlacklistedTransferTo shouldBe Set("b")
  }
}
