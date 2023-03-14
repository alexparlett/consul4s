package com.ap.consul.txn

import com.ap.consul.json.PascalAutoDerivation
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

object Txn extends PascalAutoDerivation {

  implicit val KVClassDecoder = deriveConfiguredCodec[KV]
  implicit val TxnOperationClassDecoder = deriveConfiguredCodec[TxnOperation]
  implicit val ErrorClassDecoder = deriveConfiguredCodec[Error]
  implicit val TxnResultClassDecoder = deriveConfiguredCodec[TxnResult]

  case class TxnParams(dc: Option[String] = None)

  case class KV(verb: String, index: Int, key: String, value: String)

  case class TxnOperation(kv: Option[KV] = None)

  case class Error(opIndex: Int, what: String)

  case class TxnResult(results: Option[List[TxnOperation]], errors: Option[List[Error]])
}
