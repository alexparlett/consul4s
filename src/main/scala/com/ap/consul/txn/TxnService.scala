package com.ap.consul.txn

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toBifunctorOps
import io.circe.syntax._
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.circe.jsonOf
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.{Client, UnexpectedStatus}
import org.http4s.Status.Successful

class TxnService(consulAddress: String)(implicit httpClient: Client[IO]) {
  import Txn._

  def execute(operations: List[TxnOperation],
              params: TxnParams = TxnParams()): TxnResult = {
    val uri = Uri
      .unsafeFromString(consulAddress)
      .addPath("v1/txn")
      .withOptionQueryParam("dc", params.dc)

    val request = Request[IO](Method.PUT, uri)
      .withEntity(operations.asJson)

    httpClient
      .run(request)
      .use {
        case Successful(resp) =>
          jsonOf[IO, TxnResult]
            .decode(resp, strict = false)
            .leftWiden[Throwable]
            .rethrowT
        case failedResponse =>
          failedResponse.status match {
            case Status(409) ⇒
              jsonOf[IO, TxnResult]
                .decode(failedResponse, strict = false)
                .leftWiden[Throwable]
                .rethrowT
            case _ ⇒
              IO.raiseError(
                UnexpectedStatus(
                  failedResponse.status,
                  request.method,
                  request.uri
                )
              )
          }
      }
      .unsafeRunSync()
  }

}
