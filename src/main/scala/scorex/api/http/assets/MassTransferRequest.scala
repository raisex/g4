package scorex.api.http.assets

import play.api.libs.json.Json
import scorex.transaction.transfer.MassTransferTransaction.Transfer

case class MassTransferRequest(version: Byte,
                               assetId: Option[String],
                               sender: String,
                               transfers: List[Transfer],
                               fee: Long,
                               attachment: Option[String],
                               timestamp: Option[Long] = None)

object MassTransferRequest {
  implicit val reads = Json.reads[MassTransferRequest]
}
