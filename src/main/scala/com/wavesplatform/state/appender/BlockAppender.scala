package com.wavesplatform.state.appender

import cats.data.EitherT
import com.wavesplatform.consensus.PoSSelector
import com.wavesplatform.metrics._
import com.wavesplatform.mining.Miner
import com.wavesplatform.network._
import com.wavesplatform.settings.WavesSettings
//import com.wavesplatform.state.{Blockchain, ByteStr, _}
import com.wavesplatform.state.Blockchain
import com.wavesplatform.utx.UtxPool
import io.netty.channel.Channel
import io.netty.channel.group.ChannelGroup
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import scorex.block.Block
import scorex.transaction.ValidationError.{BlockAppendError, InvalidSignature}
import scorex.transaction.{BlockchainUpdater, CheckpointService, ValidationError}
import scorex.utils.{ScorexLogging, Time}

import scala.util.Right

object BlockAppender extends ScorexLogging with Instrumented {
//  private val exceptions = List(
//    ByteStr.decodeBase58("5EmZvVfuA8QQwrnMyyGcjm1sSQ2YvcJL9aiEaAoKoJhcTxp8kYq1ADaY1qt88pfR2mkAes6BjXupiPQVZBiqQkSY").get,
//    ByteStr.decodeBase58("2E32Qc9xCgK9he71Kh7fiVgh5mjsfkHCqEdJh8n1MTEKMBKX6Dr5bdi6VuwvBApSFTo9iq3WuKpVD96zAFWniiVS").get,
//  )
  def apply(checkpoint: CheckpointService,
            blockchainUpdater: BlockchainUpdater with Blockchain,
            time: Time,
            utxStorage: UtxPool,
            pos: PoSSelector,
            settings: WavesSettings,
            scheduler: Scheduler)(newBlock: Block): Task[Either[ValidationError, Option[BigInt]]] =
    Task {
      measureSuccessful(
        blockProcessingTimeStats, {
          if (blockchainUpdater.isLastBlockId(newBlock.reference)) { //||newBlock.timestamp >= 1531517781 ||exceptions.contains(newBlock.uniqueId)
            appendBlock(checkpoint, blockchainUpdater, utxStorage, pos, time, settings)(newBlock).map(_ => Some(blockchainUpdater.score))
          } else if (blockchainUpdater.contains(newBlock.uniqueId)) {
            Right(None)
          } else {
            Left(BlockAppendError("Block is not a child of the last block", newBlock))
          }
        }
      )
    }.executeOn(scheduler)

  def apply(checkpoint: CheckpointService,
            blockchainUpdater: BlockchainUpdater with Blockchain,
            time: Time,
            utxStorage: UtxPool,
            pos: PoSSelector,
            settings: WavesSettings,
            allChannels: ChannelGroup,
            peerDatabase: PeerDatabase,
            miner: Miner,
            scheduler: Scheduler)(ch: Channel, newBlock: Block): Task[Unit] = {
    BlockStats.received(newBlock, BlockStats.Source.Broadcast, ch)
    blockReceivingLag.safeRecord(System.currentTimeMillis() - newBlock.timestamp)
    (for {
      _                <- EitherT(Task.now(newBlock.signaturesValid()))
      validApplication <- EitherT(apply(checkpoint, blockchainUpdater, time, utxStorage, pos, settings, scheduler)(newBlock))
    } yield validApplication).value.map {
      case Right(None) => // block already appended
      case Right(Some(_)) =>
        BlockStats.applied(newBlock, BlockStats.Source.Broadcast, blockchainUpdater.height)
        log.debug(s"${id(ch)} Appended $newBlock")
        if (newBlock.transactionData.isEmpty)
          allChannels.broadcast(BlockForged(newBlock), Some(ch))
        miner.scheduleMining()
      case Left(is: InvalidSignature) =>
        peerDatabase.blacklistAndClose(ch, s"Could not append $newBlock: $is")
      case Left(ve) =>
        BlockStats.declined(newBlock, BlockStats.Source.Broadcast)
        log.debug(s"${id(ch)} Could not append $newBlock: $ve")
    }
  }

  private val blockReceivingLag        = Kamon.metrics.histogram("block-receiving-lag")
  private val blockProcessingTimeStats = Kamon.metrics.histogram("single-block-processing-time")

}
