package com.ap.consul.kv

import java.util.Base64

import com.ap.consul.json.PascalAutoDerivation
import io.circe.Decoder
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.jawn.decodeByteArray

object KV extends PascalAutoDerivation {

  implicit val MetadataClassCodec = deriveConfiguredCodec[Metadata]

  case class SetParams(dc: Option[String] = None,
                       flags: Option[Int] = None,
                       cas: Option[Int] = None,
                       acquire: Option[String] = None,
                       release: Option[String] = None)

  case class GetParams(dc: Option[String] = None,
                       recurse: Option[Boolean] = None)

  case class GetRawParams(dc: Option[String] = None)

  case class GetKeysParams(dc: Option[String] = None,
                           separator: Option[String] = None)

  case class DeleteParams(dc: Option[String] = None,
                          recurse: Option[Boolean] = None,
                          cas: Option[Int] = None)

  case class Metadata(createIndex: Int,
                      modifyIndex: Int,
                      lockIndex: Int,
                      key: String,
                      flags: Int,
                      value: String,
                      session: Option[String]) {

    def map[T >: Null](implicit decoder: Decoder[T]): Option[T] = {
      decodeByteArray[T](Base64.getDecoder.decode(value)).toOption
    }
  }
}
