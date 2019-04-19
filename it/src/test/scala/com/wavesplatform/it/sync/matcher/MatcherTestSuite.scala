package com.wavesplatform.it.async.matcher

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.it.api.SyncHttpApi._
import com.wavesplatform.it.sync.matcher._
import com.wavesplatform.it.transactions.NodesFromDocker
import com.wavesplatform.it.ReportingTestName
import com.wavesplatform.it.api.LevelResponse
import com.wavesplatform.state.ByteStr
import com.wavesplatform.it.util._

import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfterAll, CancelAfterFailure, FreeSpec, Matchers}
import scorex.transaction.assets.exchange.{AssetPair, Order, OrderType}

import scala.util.Random

class MatcherTestSuite extends FreeSpec with Matchers with BeforeAndAfterAll with CancelAfterFailure with NodesFromDocker with ReportingTestName {

  import MatcherTestSuite._

  override protected def nodeConfigs: Seq[Config] = Configs

  private def matcherNode = nodes.head
  private def aliceNode   = nodes(1)
  private def bobNode     = nodes(2)

  private val aliceSellAmount = 500

  "Check cross ordering between Alice and  Bob " - {
    // Alice issues new asset
    val aliceAsset =
      aliceNode.issue(aliceNode.address, "AliceCoin", "AliceCoin for matcher's tests", AssetQuantity, 0, reissuable = false, 100000000L).id
    nodes.waitForHeightAriseAndTxPresent(aliceAsset)

    val aliceWavesPair = AssetPair(ByteStr.decodeBase58(aliceAsset).toOption, None)

    // Wait for balance on Alice's account
    aliceNode.assertAssetBalance(aliceNode.address, aliceAsset, AssetQuantity)
    matcherNode.assertAssetBalance(matcherNode.address, aliceAsset, 0)
    bobNode.assertAssetBalance(bobNode.address, aliceAsset, 0)

    "matcher should respond with Public key" in {
      matcherNode.matcherGet("/matcher").getResponseBody.stripPrefix("\"").stripSuffix("\"") shouldBe matcherNode.publicKeyStr
    }

    "sell order could be placed correctly" - {
      // Alice places sell order
      val order1 =
        matcherNode.placeOrder(
          prepareOrder(aliceNode, matcherNode, aliceWavesPair, OrderType.SELL, 2.waves * Order.PriceConstant, aliceSellAmount, 2.minutes))

      order1.status shouldBe "OrderAccepted"

      // Alice checks that the order in order book
      matcherNode.getOrderStatus(aliceAsset, order1.message.id).status shouldBe "Accepted"

      // Alice check that order is correct
      val orders = matcherNode.getOrderBook(aliceAsset)
      orders.asks.head.amount shouldBe aliceSellAmount
      orders.asks.head.price shouldBe 2.waves * Order.PriceConstant

      "frozen amount should be listed via matcherBalance REST endpoint" in {
        getReservedBalance(aliceNode, matcherNode) shouldBe Map(aliceAsset -> aliceSellAmount)

        getReservedBalance(bobNode, matcherNode) shouldBe Map()
      }

      "and should be listed by trader's publiс key via REST" in {
        getOrderBook(aliceNode, matcherNode).map(_.id) should contain(order1.message.id)
      }

      "and should match with buy order" in {
        val bobBalance     = bobNode.accountBalances(bobNode.address)._1
        val matcherBalance = matcherNode.accountBalances(matcherNode.address)._1
        val aliceBalance   = aliceNode.accountBalances(aliceNode.address)._1

        // Bob places a buy order
        val order2 = matcherNode.placeOrder(prepareOrder(bobNode, matcherNode, aliceWavesPair, OrderType.BUY, 2.waves * Order.PriceConstant, 200))
        order2.status shouldBe "OrderAccepted"

        matcherNode.waitOrderStatus(aliceAsset, order1.message.id, "PartiallyFilled")
        matcherNode.waitOrderStatus(aliceAsset, order2.message.id, "Filled")

        nodes.waitForHeightArise()

        // Bob checks that asset on his balance
        bobNode.assertAssetBalance(bobNode.address, aliceAsset, 200)

        // Alice checks that part of her order still in the order book
        val orders = matcherNode.getOrderBook(aliceAsset)
        orders.asks.head.amount shouldBe 300
        orders.asks.head.price shouldBe 2.waves * Order.PriceConstant

        // Alice checks that she sold some assets
        aliceNode.assertAssetBalance(aliceNode.address, aliceAsset, 800)

        // Bob checks that he spent some Waves
        val updatedBobBalance = bobNode.accountBalances(bobNode.address)._1
        updatedBobBalance shouldBe (bobBalance - 2.waves * 200 - MatcherFee)

        // Alice checks that she received some Waves
        val updatedAliceBalance = aliceNode.accountBalances(aliceNode.address)._1
        updatedAliceBalance shouldBe (aliceBalance + 2.waves * 200 - (MatcherFee * 200.0 / 500.0).toLong)

        // Matcher checks that it earn fees
        val updatedMatcherBalance = matcherNode.accountBalances(matcherNode.address)._1
        updatedMatcherBalance shouldBe (matcherBalance + MatcherFee + (MatcherFee * 200.0 / 500.0).toLong - TransactionFee)
      }

      "request activeOnly orders" in {
        val aliceOrders = getActiveOrderBook(aliceNode, matcherNode)
        aliceOrders.map(_.id) shouldBe Seq(order1.message.id)
        val bobOrders = getActiveOrderBook(bobNode, matcherNode)
        bobOrders.map(_.id) shouldBe Seq()
      }

      "submitting sell orders should check availability of asset" in {
        // Bob trying to place order on more assets than he has - order rejected
        val badOrder = prepareOrder(bobNode, matcherNode, aliceWavesPair, OrderType.SELL, (19.waves / 10.0 * Order.PriceConstant).toLong, 300)
        matcherNode.expectIncorrectOrderPlacement(badOrder, 400, "OrderRejected") should be(true)

        // Bob places order on available amount of assets - order accepted
        val goodOrder = prepareOrder(bobNode, matcherNode, aliceWavesPair, OrderType.SELL, (19.waves / 10.0 * Order.PriceConstant).toLong, 150)
        val order3    = matcherNode.placeOrder(goodOrder)
        order3.status should be("OrderAccepted")

        // Bob checks that the order in the order book
        val orders = matcherNode.getOrderBook(aliceAsset)
        orders.asks should contain(LevelResponse(19.waves / 10 * Order.PriceConstant, 150))
      }

      "buy order should match on few price levels" in {
        val matcherBalance = matcherNode.accountBalances(matcherNode.address)._1
        val aliceBalance   = aliceNode.accountBalances(aliceNode.address)._1
        val bobBalance     = bobNode.accountBalances(bobNode.address)._1

        // Alice places a buy order
        val order4 = matcherNode.placeOrder(
          prepareOrder(aliceNode, matcherNode, aliceWavesPair, OrderType.BUY, (21.waves / 10.0 * Order.PriceConstant).toLong, 350))
        order4.status should be("OrderAccepted")

        // Where were 2 sells that should fulfill placed order
        matcherNode.waitOrderStatus(aliceAsset, order4.message.id, "Filled")

        // Check balances
        nodes.waitForHeightArise()
        aliceNode.assertAssetBalance(aliceNode.address, aliceAsset, 950)
        bobNode.assertAssetBalance(bobNode.address, aliceAsset, 50)

        val updatedMatcherBalance = matcherNode.accountBalances(matcherNode.address)._1
        updatedMatcherBalance should be(
          matcherBalance - 2 * TransactionFee + MatcherFee + (MatcherFee * 150.0 / 350.0).toLong + (MatcherFee * 200.0 / 350.0).toLong + (MatcherFee * 200.0 / 500.0).toLong)

        val updatedBobBalance = bobNode.accountBalances(bobNode.address)._1
        updatedBobBalance should be(bobBalance - MatcherFee + 150 * (19.waves / 10.0).toLong)

        val updatedAliceBalance = aliceNode.accountBalances(aliceNode.address)._1
        updatedAliceBalance should be(
          aliceBalance - (MatcherFee * 200.0 / 350.0).toLong - (MatcherFee * 150.0 / 350.0).toLong - (MatcherFee * 200.0 / 500.0).toLong - (19.waves / 10.0).toLong * 150)
      }

      "order could be canceled and resubmitted again" in {
        // Alice cancels the very first order (100 left)
        val status1 = matcherCancelOrder(aliceNode, matcherNode, aliceWavesPair, order1.message.id)
        status1.status should be("OrderCanceled")

        // Alice checks that the order book is empty
        val orders1 = matcherNode.getOrderBook(aliceAsset)
        orders1.asks.size should be(0)
        orders1.bids.size should be(0)

        // Alice places a new sell order on 100
        val order4 =
          matcherNode.placeOrder(prepareOrder(aliceNode, matcherNode, aliceWavesPair, OrderType.SELL, 2.waves * Order.PriceConstant, 100))
        order4.status should be("OrderAccepted")

        // Alice checks that the order is in the order book
        val orders2 = matcherNode.getOrderBook(aliceAsset)
        orders2.asks should contain(LevelResponse(20.waves / 10 * Order.PriceConstant, 100))
      }

      "buy order should execute all open orders and put remaining in order book" in {
        val matcherBalance = matcherNode.accountBalances(matcherNode.address)._1
        val aliceBalance   = aliceNode.accountBalances(aliceNode.address)._1
        val bobBalance     = bobNode.accountBalances(bobNode.address)._1

        // Bob places buy order on amount bigger then left in sell orders
        val order5 = matcherNode.placeOrder(prepareOrder(bobNode, matcherNode, aliceWavesPair, OrderType.BUY, 2.waves * Order.PriceConstant, 130))
        order5.status should be("OrderAccepted")

        // Check that the order is partially filled
        matcherNode.waitOrderStatus(aliceAsset, order5.message.id, "PartiallyFilled")

        // Check that remaining part of the order is in the order book
        val orders = matcherNode.getOrderBook(aliceAsset)
        orders.bids should contain(LevelResponse(2.waves * Order.PriceConstant, 30))

        // Check balances
        nodes.waitForHeightArise()
        aliceNode.assertAssetBalance(aliceNode.address, aliceAsset, 850)
        bobNode.assertAssetBalance(bobNode.address, aliceAsset, 150)

        val updatedMatcherBalance = matcherNode.accountBalances(matcherNode.address)._1
        updatedMatcherBalance should be(matcherBalance - TransactionFee + MatcherFee + (MatcherFee * 100.0 / 130.0).toLong)

        val updatedBobBalance = bobNode.accountBalances(bobNode.address)._1
        updatedBobBalance should be(bobBalance - (MatcherFee * 100.0 / 130.0).toLong - 100 * 2.waves)

        val updatedAliceBalance = aliceNode.accountBalances(aliceNode.address)._1
        updatedAliceBalance should be(aliceBalance - MatcherFee + 2.waves * 100)
      }

      "request order book for blacklisted pair" in {
        val f = matcherNode.matcherGetStatusCode(s"/matcher/orderbook/$ForbiddenAssetId/TN", 404)
        f.message shouldBe s"Invalid Asset ID: $ForbiddenAssetId"
      }

      "should consider UTX pool when checking the balance" in {
        // Bob issues new asset
        val bobAssetQuantity = 10000
        val bobAssetName     = "BobCoin"
        val bobAsset         = bobNode.issue(bobNode.address, bobAssetName, "Bob's asset", bobAssetQuantity, 0, false, 100000000L).id
        nodes.waitForHeightAriseAndTxPresent(bobAsset)

        aliceNode.assertAssetBalance(aliceNode.address, bobAsset, 0)
        matcherNode.assertAssetBalance(matcherNode.address, bobAsset, 0)
        bobNode.assertAssetBalance(bobNode.address, bobAsset, bobAssetQuantity)

        // Bob wants to sell all own assets for 1 Wave
        val bobWavesPair = AssetPair(ByteStr.decodeBase58(bobAsset).toOption, None)

        def bobOrder = prepareOrder(bobNode, matcherNode, bobWavesPair, OrderType.SELL, 1.waves * Order.PriceConstant, bobAssetQuantity)

        val order6 = matcherNode.placeOrder(bobOrder)
        matcherNode.waitOrderStatus(bobAsset, order6.message.id, "Accepted")

        // Alice wants to buy all Bob's assets for 1 Wave
        val order7 =
          matcherNode.placeOrder(prepareOrder(aliceNode, matcherNode, bobWavesPair, OrderType.BUY, 1.waves * Order.PriceConstant, bobAssetQuantity))
        matcherNode.waitOrderStatus(bobAsset, order7.message.id, "Filled")

        // Bob tries to do the same operation, but at now he have no assets
        matcherNode.expectIncorrectOrderPlacement(bobOrder, 400, "OrderRejected")
      }

      "trader can buy TN for assets with order without having TN" in {
        // Bob issues new asset
        val bobAssetQuantity = 10000
        val bobAssetName     = "BobCoin2"
        val bobAsset         = bobNode.issue(bobNode.address, bobAssetName, "Bob's asset", bobAssetQuantity, 0, false, 100000000L).id
        nodes.waitForHeightAriseAndTxPresent(bobAsset)

        val bobWavesPair = AssetPair(
          amountAsset = ByteStr.decodeBase58(bobAsset).toOption,
          priceAsset = None
        )

        aliceNode.assertAssetBalance(aliceNode.address, bobAsset, 0)
        matcherNode.assertAssetBalance(matcherNode.address, bobAsset, 0)
        bobNode.assertAssetBalance(bobNode.address, bobAsset, bobAssetQuantity)

        // Bob wants to sell all own assets for 1 Wave
        def bobOrder = prepareOrder(bobNode, matcherNode, bobWavesPair, OrderType.SELL, 1.waves * Order.PriceConstant, bobAssetQuantity)

        val order8 = matcherNode.placeOrder(bobOrder)
        matcherNode.waitOrderStatus(bobAsset, order8.message.id, "Accepted")

        // Bob moves all waves to Alice
        val h1              = matcherNode.height
        val bobBalance      = matcherNode.accountBalances(bobNode.address)._1
        val transferAmount  = bobBalance - TransactionFee
        val transferAliceId = bobNode.transfer(bobNode.address, aliceNode.address, transferAmount, TransactionFee, None, None).id
        nodes.waitForHeightAriseAndTxPresent(transferAliceId)

        matcherNode.accountBalances(bobNode.address)._1 shouldBe 0

        // Order should stay accepted
        matcherNode.waitForHeight(h1 + 5, 2.minutes)
        matcherNode.waitOrderStatus(bobAsset, order8.message.id, "Accepted")

        // Cleanup
        nodes.waitForHeightArise()
        matcherCancelOrder(bobNode, matcherNode, bobWavesPair, order8.message.id).status should be("OrderCanceled")

        val transferBobId = aliceNode.transfer(aliceNode.address, bobNode.address, transferAmount, TransactionFee, None, None).id
        nodes.waitForHeightAriseAndTxPresent(transferBobId)

      }

    }

  }
}

object MatcherTestSuite {

  import ConfigFactory._
  import com.wavesplatform.it.NodeConfigs._

  private val ForbiddenAssetId = "FdbnAsset"
  private val AssetQuantity    = 1000
  private val MatcherFee       = 300000
  private val TransactionFee   = 300000

  private val minerDisabled = parseString("TN.miner.enable = no")
  private val matcherConfig = parseString(s"""
       |TN.matcher {
       |  enable = yes
       |  account = 3HmFkAoQRs4Y3PE2uR6ohN7wS4VqPBGKv7k
       |  bind-address = "0.0.0.0"
       |  order-match-tx-fee = 300000
       |  blacklisted-assets = ["$ForbiddenAssetId"]
       |  balance-watching.enable = yes
       |}""".stripMargin)

  private val Configs: Seq[Config] = (Default.last +: Random.shuffle(Default.init).take(3))
    .zip(Seq(matcherConfig, minerDisabled, minerDisabled, empty()))
    .map { case (n, o) => o.withFallback(n) }
}
