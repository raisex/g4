package com.wavesplatform.it.sync.smartcontract

import com.wavesplatform.crypto
import com.wavesplatform.it.api.SyncHttpApi._
import com.wavesplatform.it.sync._
import com.wavesplatform.it.transactions.BaseTransactionSuite
import com.wavesplatform.lang.v1.compiler.CompilerV1
import com.wavesplatform.lang.v1.parser.Parser
import com.wavesplatform.state._
import com.wavesplatform.utils.dummyCompilerContext
import org.scalatest.CancelAfterFailure
import play.api.libs.json.JsNumber
import scorex.account.PrivateKeyAccount
import scorex.transaction.Proofs
import scorex.transaction.smart.SetScriptTransaction
import scorex.transaction.smart.script.v1.ScriptV1
import scorex.transaction.transfer._

/*
Scenario:
1. Alice's and swapBC1's balances initialisation
2. Create and setup smart contract for swapBC1
3. Alice funds swapBC1t
4. Alice can't take money from swapBC1
5.1 Bob takes funds because he knows secret hash OR 5.2 Wait height and Alice takes funds back
 */

class AtomicSwapSmartContractSuite extends BaseTransactionSuite with CancelAfterFailure {

  private val BobBC1: String   = sender.createAddress()
  private val AliceBC1: String = sender.createAddress()
  private val swapBC1: String  = sender.createAddress()

  private val AlicesPK = PrivateKeyAccount.fromSeed(sender.seed(AliceBC1)).explicitGet()

  private val secretText = "some secret message from Alice"
  private val shaSecret  = "BN6RTYGWcwektQfSFzH8raYo9awaLgQ7pLyWLQY4S4F5"

  test("step1: Balances initialization") {
    val toAliceBC1TxId = sender.transfer(sender.address, AliceBC1, 10 * transferAmount, minFee).id
    nodes.waitForHeightAriseAndTxPresent(toAliceBC1TxId)

    val toSwapBC1TxId = sender.transfer(sender.address, swapBC1, minFee, minFee).id
    nodes.waitForHeightAriseAndTxPresent(toSwapBC1TxId)
  }

  test("step2: Create and setup smart contract for swapBC1") {
    val beforeHeight = sender.height
    val sc1 = {
      val untyped = Parser(s"""
    let Bob = Address(base58'$BobBC1')
    let Alice = Address(base58'$AliceBC1')

    match tx {
      case ttx: TransferTransaction =>
        let txToBob = (ttx.recipient == Bob) && (sha256(ttx.proofs[0]) == base58'$shaSecret') && ((20 + $beforeHeight) >= height)
        let backToAliceAfterHeight = ((height >= (21 + $beforeHeight)) && (ttx.recipient == Alice))

        txToBob || backToAliceAfterHeight
      case other => false
    }""".stripMargin).get.value
      assert(untyped.size == 1)
      CompilerV1(dummyCompilerContext, untyped.head).explicitGet()._1
    }

    val pkSwapBC1 = PrivateKeyAccount.fromSeed(sender.seed(swapBC1)).explicitGet()
    val script    = ScriptV1(sc1).explicitGet()
    val sc1SetTx = SetScriptTransaction
      .selfSigned(version = SetScriptTransaction.supportedVersions.head,
                  sender = pkSwapBC1,
                  script = Some(script),
                  fee = minFee,
                  timestamp = System.currentTimeMillis())
      .explicitGet()

    val setScriptId = sender
      .signedBroadcast(sc1SetTx.json() + ("type" -> JsNumber(SetScriptTransaction.typeId.toInt)))
      .id

    nodes.waitForHeightAriseAndTxPresent(setScriptId)

    val swapBC1ScriptInfo = sender.addressScriptInfo(swapBC1)

    swapBC1ScriptInfo.script.isEmpty shouldBe false
    swapBC1ScriptInfo.scriptText.isEmpty shouldBe false
  }

  test("step3: Alice makes transfer to swapBC1") {
    val txToSwapBC1 =
      TransferTransactionV2
        .selfSigned(
          version = 2,
          assetId = None,
          sender = PrivateKeyAccount.fromSeed(sender.seed(AliceBC1)).explicitGet(),
          recipient = PrivateKeyAccount.fromSeed(sender.seed(swapBC1)).explicitGet(),
          amount = transferAmount + minFee + smartFee,
          timestamp = System.currentTimeMillis(),
          feeAssetId = None,
          feeAmount = minFee + smartFee,
          attachment = Array.emptyByteArray
        )
        .explicitGet()

    val transferId = sender
      .signedBroadcast(txToSwapBC1.json() + ("type" -> JsNumber(TransferTransactionV2.typeId.toInt)))
      .id
    nodes.waitForHeightAriseAndTxPresent(transferId)
  }

  test("step4: Alice cannot make transfer from swapBC1 if height is incorrect") {
    val txToSwapBC1 =
      TransferTransactionV2
        .selfSigned(
          version = 2,
          assetId = None,
          sender = PrivateKeyAccount.fromSeed(sender.seed(swapBC1)).explicitGet(),
          recipient = PrivateKeyAccount.fromSeed(sender.seed(AliceBC1)).explicitGet(),
          amount = transferAmount,
          timestamp = System.currentTimeMillis(),
          feeAssetId = None,
          feeAmount = minFee + smartFee,
          attachment = Array.emptyByteArray
        )
        .explicitGet()

    assertBadRequest(sender.signedBroadcast(txToSwapBC1.json() + ("type" -> JsNumber(TransferTransactionV2.typeId.toInt))))
  }

  test("step5: Bob makes transfer; after revert Alice takes funds back") {
    val height = sender.height

    val (bobBalance, bobEffBalance)     = notMiner.accountBalances(BobBC1)
    val (aliceBalance, aliceEffBalance) = notMiner.accountBalances(AliceBC1)
    val (swapBalance, swapEffBalance)   = notMiner.accountBalances(swapBC1)

    val unsigned =
      TransferTransactionV2
        .create(
          version = 2,
          assetId = None,
          sender = PrivateKeyAccount.fromSeed(sender.seed(swapBC1)).explicitGet(),
          recipient = PrivateKeyAccount.fromSeed(sender.seed(BobBC1)).explicitGet(),
          amount = transferAmount,
          timestamp = System.currentTimeMillis(),
          feeAssetId = None,
          feeAmount = minFee + smartFee,
          attachment = Array.emptyByteArray,
          proofs = Proofs.empty
        )
        .explicitGet()

    val proof    = ByteStr(secretText.getBytes())
    val sigAlice = ByteStr(crypto.sign(AlicesPK, unsigned.bodyBytes()))
    val signed   = unsigned.copy(proofs = Proofs(Seq(proof, sigAlice)))
    val versionedTransferId =
      sender.signedBroadcast(signed.json() + ("type" -> JsNumber(TransferTransactionV2.typeId.toInt))).id

    nodes.waitForHeightAriseAndTxPresent(versionedTransferId)

    notMiner.assertBalances(swapBC1, swapBalance - transferAmount - (minFee + smartFee), swapEffBalance - transferAmount - (minFee + smartFee))
    notMiner.assertBalances(BobBC1, bobBalance + transferAmount, bobEffBalance + transferAmount)
    notMiner.assertBalances(AliceBC1, aliceBalance, aliceEffBalance)

    nodes.rollback(height, false)

    nodes.waitForHeight(height + 20)

    val selfSignedToAlice = TransferTransactionV2
      .selfSigned(
        version = 2,
        assetId = None,
        sender = PrivateKeyAccount.fromSeed(sender.seed(swapBC1)).explicitGet(),
        recipient = PrivateKeyAccount.fromSeed(sender.seed(AliceBC1)).explicitGet(),
        amount = transferAmount,
        timestamp = System.currentTimeMillis(),
        feeAssetId = None,
        feeAmount = minFee + smartFee,
        attachment = Array.emptyByteArray
      )
      .explicitGet()

    val transferToAlice =
      sender.signedBroadcast(selfSignedToAlice.json() + ("type" -> JsNumber(TransferTransactionV2.typeId.toInt))).id
    nodes.waitForHeightAriseAndTxPresent(transferToAlice)

    notMiner.assertBalances(swapBC1, swapBalance - transferAmount - (minFee + smartFee), swapEffBalance - transferAmount - (minFee + smartFee))
    notMiner.assertBalances(BobBC1, bobBalance, bobEffBalance)
    notMiner.assertBalances(AliceBC1, aliceBalance + transferAmount, aliceEffBalance + transferAmount)
  }

}
