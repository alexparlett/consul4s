package com.ap.consul.kv

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.{Decoder, Encoder}
import io.circe.syntax._
import org.http4s.{Method, Request, Uri}
import org.http4s.circe.jsonOf
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client

class KVService(consulAddress: String)(implicit httpClient: Client[IO]) {
  import KV._

  def set[T](key: String, value: T, params: SetParams = SetParams())(
    implicit encoder: Encoder[T]
  ): Boolean = {
    val uri = Uri
      .unsafeFromString(consulAddress)
      .addPath("v1/kv")
      .addPath(key)
      .withOptionQueryParam("dc", params.dc)
      .withOptionQueryParam("flags", params.flags)
      .withOptionQueryParam("cas", params.cas)
      .withOptionQueryParam("acquire", params.acquire)
      .withOptionQueryParam("release", params.release)

    val request = Request[IO](Method.PUT, uri)
      .withEntity(value.asJson)

    httpClient.expect(request)(jsonOf[IO, Boolean]).unsafeRunSync()
  }

  def get[T >: Null](key: String,
                     params: GetParams = GetParams()): List[Metadata] = {
    val uri = Uri
      .unsafeFromString(consulAddress)
      .addPath("v1/kv")
      .addPath(key)
      .withOptionQueryParam("dc", params.dc)
      .withOptionQueryParam("recurse", params.recurse)

    val request = Request[IO](Method.GET, uri)

    httpClient
      .expect(request)(jsonOf[IO, List[Metadata]])
      .unsafeRunSync()
  }

  def raw[T >: Null](key: String, params: GetRawParams = GetRawParams())(
    implicit decoder: Decoder[T]
  ): T = {
    val uri = Uri
      .unsafeFromString(consulAddress)
      .addPath("v1/kv")
      .addPath(key)
      .withOptionQueryParam("dc", params.dc)
      .withQueryParam("raw", true)

    val request = Request[IO](Method.GET, uri)

    httpClient.expect(request)(jsonOf[IO, T]).unsafeRunSync()
  }

  def keys(key: String,
           params: GetKeysParams = GetKeysParams()): List[String] = {
    val uri = Uri
      .unsafeFromString(consulAddress)
      .addPath("v1/kv")
      .addPath(key)
      .withQueryParam("keys", true)
      .withOptionQueryParam("dc", params.dc)
      .withOptionQueryParam("separator", params.separator)

    val request = Request[IO](Method.GET, uri)

    httpClient
      .expect(request)(jsonOf[IO, List[String]])
      .unsafeRunSync()
  }

  def delete(key: String, params: DeleteParams = DeleteParams()): Boolean = {
    val uri = Uri
      .unsafeFromString(consulAddress)
      .addPath("v1/kv")
      .addPath(key)
      .withOptionQueryParam("dc", params.dc)
      .withOptionQueryParam("recurse", params.recurse)
      .withOptionQueryParam("cas", params.cas)

    val request = Request[IO](Method.DELETE, uri)

    httpClient
      .expect(request)(jsonOf[IO, Boolean])
      .unsafeRunSync()
  }

}
