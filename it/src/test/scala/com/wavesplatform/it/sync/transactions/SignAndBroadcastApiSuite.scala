package com.wavesplatform.it.sync.transactions

import com.wavesplatform.crypto
import com.wavesplatform.it.api.SyncHttpApi._
import com.wavesplatform.it.sync.{issueFee, someAssetAmount}
import com.wavesplatform.it.transactions.BaseTransactionSuite
import com.wavesplatform.it.util._
import com.wavesplatform.state._
import org.asynchttpclient.util.HttpConstants
import play.api.libs.json._
import scorex.account.{PrivateKeyAccount, PublicKeyAccount}
import scorex.api.http.assets.SignedTransferV1Request
import com.wavesplatform.utils.Base58
import scorex.transaction.assets.exchange.{AssetPair, ExchangeTransaction, Order}
import com.wavesplatform.it.sync._
import scorex.transaction.transfer.MassTransferTransaction.Transfer
import scorex.utils.NTP

import scala.util.Random

class SignAndBroadcastApiSuite extends BaseTransactionSuite {
  test("height should always be reported for transactions") {
    val txId = sender.transfer(firstAddress, secondAddress, 1.TN, fee = 1.TN).id
    nodes.waitForHeightAriseAndTxPresent(txId)

    val jsv1               = Json.parse((sender.get(s"/transactions/info/$txId")).getResponseBody)
    val hasPositiveHeight1 = (jsv1 \ "height").asOpt[Int].map(_ > 0)
    assert(hasPositiveHeight1.getOrElse(false))

    val response           = sender.get(s"/transactions/address/$firstAddress/limit/1")
    val jsv2               = Json.parse(response.getResponseBody).as[JsArray]
    val hasPositiveHeight2 = (jsv2(0)(0) \ "height").asOpt[Int].map(_ > 0)

    assert(hasPositiveHeight2.getOrElse(false))
  }

  test("/transactions/sign should handle erroneous input") {
    def assertSignBadJson(json: JsObject, expectedMessage: String) =
      assertBadRequestAndMessage(sender.postJsonWithApiKey("/transactions/sign", json), expectedMessage)

    for (v <- supportedVersions) {
      val json = Json.obj("type" -> 10, "sender" -> firstAddress, "alias" -> "alias", "fee" -> 100000)
      val js   = if (Option(v).isDefined) json ++ Json.obj("version" -> v.toInt) else json
      assertSignBadJson(js - "type", "failed to parse json message")
      assertSignBadJson(js + ("type" -> Json.toJson(-100)), "Bad transaction type")
      assertSignBadJson(js - "alias", "failed to parse json message")
    }
  }

  test("/transactions/sign should respect timestamp if specified") {
    val timestamp = 1500000000000L
    for (v <- supportedVersions) {
      val json = Json.obj("type" -> 10, "sender" -> firstAddress, "alias" -> "alias", "fee" -> 100000, "timestamp" -> timestamp)
      val js   = if (Option(v).isDefined) json ++ Json.obj("version" -> v.toInt) else json
      val r    = sender.postJsonWithApiKey("/transactions/sign", js)
      assert(r.getStatusCode == HttpConstants.ResponseStatusCodes.OK_200)
      assert((Json.parse(r.getResponseBody) \ "timestamp").as[Long] == timestamp)
    }
  }

  test("/transactions/broadcast should handle erroneous input") {
    def assertBroadcastBadJson(json: JsObject, expectedMessage: String) =
      assertBadRequestAndMessage(sender.postJson("/transactions/broadcast", json), expectedMessage)

    val timestamp = System.currentTimeMillis
    val jsonV1 = Json.obj(
      "type"            -> 10,
      "senderPublicKey" -> "8LbAU5BSrGkpk5wbjLMNjrbc9VzN9KBBYv9X8wGpmAJT",
      "alias"           -> "alias",
      "fee"             -> 100000,
      "timestamp"       -> timestamp,
      "signature"       -> "A" * 64
    )

    assertBroadcastBadJson(jsonV1, "invalid signature")

    val jsonV2 = Json.obj(
      "type"            -> 10,
      "version"         -> 2,
      "senderPublicKey" -> "8LbAU5BSrGkpk5wbjLMNjrbc9VzN9KBBYv9X8wGpmAJT",
      "alias"           -> "alias",
      "fee"             -> 100000,
      "timestamp"       -> timestamp,
      "proofs"          -> List("A" * 64)
    )

    assertBroadcastBadJson(jsonV2, "Script doesn't exist and proof doesn't validate")

    for (j <- List(jsonV1, jsonV2)) {
      assertBroadcastBadJson(j - "type", "failed to parse json message")
      assertBroadcastBadJson(j - "type" + ("type" -> Json.toJson(88)), "Bad transaction type")
      assertBroadcastBadJson(j - "alias", "failed to parse json message")
    }
  }

  test("/transactions/sign should produce issue/reissue/burn/transfer transactions that are good for /transactions/broadcast") {
    for (v <- supportedVersions) {
      val isProof = Option(v).nonEmpty
      val issueId = signAndBroadcast(
        Json.obj("type"        -> 3,
                 "name"        -> "Gigacoin",
                 "quantity"    -> 100.TN,
                 "description" -> "Gigacoin",
                 "sender"      -> firstAddress,
                 "decimals"    -> 8,
                 "reissuable"  -> true,
                 "fee"         -> 1.TN),
        usesProofs = isProof,
        version = v
      )

      signAndBroadcast(
        Json.obj("type" -> 5, "quantity" -> 200.TN, "assetId" -> issueId, "sender" -> firstAddress, "reissuable" -> false, "fee" -> 1.TN),
        usesProofs = isProof,
        version = v
      )

      signAndBroadcast(Json.obj("type" -> 6, "quantity" -> 0, "assetId" -> issueId, "sender" -> firstAddress, "fee" -> 1.TN),
                       usesProofs = isProof,
                       version = v)

      signAndBroadcast(Json.obj("type" -> 6, "quantity" -> 100.TN, "assetId" -> issueId, "sender" -> firstAddress, "fee" -> 1.TN),
                       usesProofs = isProof,
                       version = v)

      signAndBroadcast(
        Json.obj(
          "type"       -> 4,
          "sender"     -> firstAddress,
          "recipient"  -> secondAddress,
          "fee"        -> 100000,
          "assetId"    -> issueId,
          "amount"     -> 1.TN,
          "attachment" -> Base58.encode("asset transfer".getBytes)
        ),
        usesProofs = isProof,
        version = v
      )
    }
  }

  test("/transactions/sign should produce transfer transaction that is good for /transactions/broadcast") {
    for (v <- supportedVersions) {
      signAndBroadcast(
        Json.obj("type"       -> 4,
                 "sender"     -> firstAddress,
                 "recipient"  -> secondAddress,
                 "fee"        -> 100000,
                 "amount"     -> 1.TN,
                 "attachment" -> Base58.encode("falafel".getBytes)),
        usesProofs = Option(v).nonEmpty,
        version = v
      )
    }
  }

  test("/transactions/sign should produce mass transfer transaction that is good for /transactions/broadcast") {
    signAndBroadcast(
      Json.obj(
        "type"       -> 11,
        "version"    -> 1,
        "sender"     -> firstAddress,
        "transfers"  -> Json.toJson(Seq(Transfer(secondAddress, 1.TN), Transfer(thirdAddress, 2.TN))),
        "fee"        -> 200000,
        "attachment" -> Base58.encode("masspay".getBytes)
      ),
      usesProofs = true
    )
  }

  test("/transactions/sign should produce lease/cancel transactions that are good for /transactions/broadcast") {
    for (v <- supportedVersions) {
      val isProof = Option(v).nonEmpty
      val leaseId =
        signAndBroadcast(Json.obj("type" -> 8, "sender" -> firstAddress, "amount" -> 1.TN, "recipient" -> secondAddress, "fee" -> 100000),
                         usesProofs = isProof,
                         version = v)

      signAndBroadcast(Json.obj("type" -> 9, "sender" -> firstAddress, "txId" -> leaseId, "fee" -> 100000), usesProofs = isProof, version = v)
    }
  }

  test("/transactions/sign should produce alias transaction that is good for /transactions/broadcast") {
    for (v <- supportedVersions) {
      val isProof = Option(v).nonEmpty
      val rnd     = Random.alphanumeric.take(9).mkString.toLowerCase
      signAndBroadcast(Json.obj("type" -> 10, "sender" -> firstAddress, "alias" -> s"myalias${rnd}", "fee" -> 100000),
                       usesProofs = isProof,
                       version = v)
    }
  }

  test("/transactions/sign should produce data transaction that is good for /transactions/broadcast") {
    signAndBroadcast(
      Json.obj(
        "type"    -> 12,
        "version" -> 1,
        "sender"  -> firstAddress,
        "data" -> List(
          IntegerDataEntry("int", 923275292849183L),
          BooleanDataEntry("bool", true),
          BinaryDataEntry("blob", ByteStr(Array.tabulate(445)(_.toByte))),
          StringDataEntry("str", "AAA-AAA")
        ),
        "fee" -> 100000
      ),
      usesProofs = true
    )
  }

  test("/transactions/sign should produce script transaction that is good for /transactions/broadcast") {
    signAndBroadcast(
      Json.obj(
        "type"    -> 13,
        "version" -> 1,
        "sender"  -> firstAddress,
        "script"  -> None,
        "fee"     -> 100000
      ),
      usesProofs = true
    )
  }

  test("/transactions/sign should produce sponsor transactions that are good for /transactions/broadcast") {
    for (v <- supportedVersions) {
      val isProof = Option(v).nonEmpty

      val assetId = signAndBroadcast(
        Json.obj(
          "type"        -> 3,
          "name"        -> "Sponsored Coin",
          "quantity"    -> 100.TN,
          "description" -> "Sponsored Coin",
          "sender"      -> firstAddress,
          "decimals"    -> 2,
          "reissuable"  -> false,
          "fee"         -> 1.TN
        ),
        usesProofs = isProof,
        version = v
      )

      signAndBroadcast(
        Json.obj(
          "type"                 -> 14,
          "version"              -> 1,
          "sender"               -> firstAddress,
          "assetId"              -> assetId,
          "minSponsoredAssetFee" -> 100,
          "fee"                  -> 1.TN
        ),
        usesProofs = true
      )

      signAndBroadcast(
        Json.obj(
          "type"                 -> 14,
          "version"              -> 1,
          "sender"               -> firstAddress,
          "assetId"              -> assetId,
          "minSponsoredAssetFee" -> JsNull,
          "fee"                  -> 1.TN
        ),
        usesProofs = true
      )
    }
  }

  test("/transactions/sign/{signerAddress} should sign a transaction by key of signerAddress") {
    val json = Json.obj(
      "type"      -> 4,
      "sender"    -> firstAddress,
      "recipient" -> secondAddress,
      "fee"       -> 100000,
      "amount"    -> 1.TN
    )

    val signedRequestResponse = sender.postJsonWithApiKey(s"/transactions/sign/$thirdAddress", json)
    assert(signedRequestResponse.getStatusCode == HttpConstants.ResponseStatusCodes.OK_200)
    val signedRequestJson = Json.parse(signedRequestResponse.getResponseBody)
    val signedRequest     = signedRequestJson.as[SignedTransferV1Request]
    assert(PublicKeyAccount.fromBase58String(signedRequest.senderPublicKey).explicitGet().address == firstAddress)
    assert(signedRequest.recipient == secondAddress)
    assert(signedRequest.fee == 100000)
    assert(signedRequest.amount == 1.TN)
    val signature  = Base58.decode((signedRequestJson \ "signature").as[String]).get
    val tx         = signedRequest.toTx.explicitGet()
    val seed       = sender.seed(thirdAddress)
    val privateKey = PrivateKeyAccount.fromSeed(seed).explicitGet()
    assert(crypto.verify(signature, tx.bodyBytes(), privateKey.publicKey))
  }

  test("/transactions/broadcast should produce ExchangeTransaction with custom asset") {
    def pkFromAddress(address: String) = PrivateKeyAccount.fromSeed(sender.seed(address)).explicitGet()

    val issueTx = signAndBroadcast(
      Json.obj(
        "type"        -> 3,
        "name"        -> "ExchangeCoin",
        "quantity"    -> 1000 * someAssetAmount,
        "description" -> "ExchangeCoin Description",
        "sender"      -> firstAddress,
        "decimals"    -> 2,
        "reissuable"  -> true,
        "fee"         -> issueFee
      ),
      usesProofs = false
    )

    val buyer               = pkFromAddress(firstAddress)
    val seller              = pkFromAddress(secondAddress)
    val matcher             = pkFromAddress(thirdAddress)
    val time                = NTP.correctedTime()
    val expirationTimestamp = time + Order.MaxLiveTime
    val buyPrice            = 1 * Order.PriceConstant
    val sellPrice           = (0.50 * Order.PriceConstant).toLong
    val mf                  = 300000L
    val buyAmount           = 2
    val sellAmount          = 3
    val assetPair           = AssetPair.createAssetPair("WAVES", issueTx).get
    val buy                 = Order.buy(buyer, matcher, assetPair, buyPrice, buyAmount, time, expirationTimestamp, mf)
    val sell                = Order.sell(seller, matcher, assetPair, sellPrice, sellAmount, time, expirationTimestamp, mf)

    val amount = math.min(buy.amount, sell.amount)
    val tx = ExchangeTransaction
      .create(
        matcher = matcher,
        buyOrder = buy,
        sellOrder = sell,
        price = sellPrice,
        amount = amount,
        buyMatcherFee = (BigInt(mf) * amount / buy.amount).toLong,
        sellMatcherFee = (BigInt(mf) * amount / sell.amount).toLong,
        fee = mf,
        timestamp = NTP.correctedTime()
      )
      .explicitGet()
      .json()

    val txId = sender.signedBroadcast(tx).id
    nodes.waitForHeightAriseAndTxPresent(txId)

  }

  private def signAndBroadcast(json: JsObject, usesProofs: Boolean, version: String = null): String = {
    val js = if (Option(version).isDefined) json ++ Json.obj("version" -> version.toInt) else json
    val rs = sender.postJsonWithApiKey("/transactions/sign", js)
    assert(rs.getStatusCode == HttpConstants.ResponseStatusCodes.OK_200)
    val body = Json.parse(rs.getResponseBody)
    val signed: Boolean = if (usesProofs) {
      val proofs = (body \ "proofs").as[Seq[String]]
      proofs.lengthCompare(1) == 0 && proofs.head.nonEmpty
    } else (body \ "signature").as[String].nonEmpty
    assert(signed)
    val rb = sender.postJson("/transactions/broadcast", body)
    assert(rb.getStatusCode == HttpConstants.ResponseStatusCodes.OK_200)
    val id = (Json.parse(rb.getResponseBody) \ "id").as[String]
    assert(id.nonEmpty)
    nodes.waitForHeightAriseAndTxPresent(id)
    id
  }
}
