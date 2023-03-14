package com.ap.consul.txn

import java.net.UnknownHostException
import java.util.Base64

import cats.effect.IO
import com.ap.consul.txn.Txn.{KV, TxnOperation}
import com.ap.consul.util.EmbeddedConsul
import io.circe.generic.AutoDerivation
import org.http4s.client.{Client, JavaNetClientBuilder}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.junit.JUnitRunner
import org.testcontainers.consul.ConsulContainer

object TxnServiceTest extends AutoDerivation {
  case class Person(name: String)
}

@RunWith(classOf[JUnitRunner])
class TxnServiceTest extends FunSuite with Matchers with EmbeddedConsul {

  private implicit val client: Client[IO] = JavaNetClientBuilder[IO].create

  test("testExecute") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new TxnService(getConsulAddress)

      val result = underTest.execute(
        List(
          TxnOperation(
            kv = Some(
              KV(
                verb = "cas",
                index = 0,
                key = "people/foo",
                value = Base64.getEncoder
                  .encodeToString("""{"name":"foo"}""".getBytes)
              )
            )
          ),
          TxnOperation(
            kv = Some(
              KV(
                verb = "cas",
                index = 0,
                key = "people/bar",
                value = Base64.getEncoder
                  .encodeToString("""{"name":"bar"}""".getBytes)
              )
            )
          )
        )
      )

      result should not be (null)
      result.results.get should have size (2)
      result.errors should be(empty)
    }
  }

  test("testExecute failed transaction") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new TxnService(getConsulAddress)

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/bar",
        """{"name":"bar"}"""
      )

      val result = underTest.execute(
        List(
          TxnOperation(
            kv = Some(
              KV(
                verb = "cas",
                index = 0,
                key = "people/foo",
                value = Base64.getEncoder
                  .encodeToString("""{"name":"foo"}""".getBytes)
              )
            )
          ),
          TxnOperation(
            kv = Some(
              KV(
                verb = "cas",
                index = 0,
                key = "people/bar",
                value = Base64.getEncoder
                  .encodeToString("""{"name":"bar"}""".getBytes)
              )
            )
          )
        )
      )

      result should not be (null)
      result.results should be(empty)
      result.errors.get should have size (1)
    }
  }

  test("testExecute other error") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new TxnService("http://fake-address")

      a[UnknownHostException] should be thrownBy {
        underTest.execute(
          List(
            TxnOperation(
              kv = Some(
                KV(
                  verb = "cas",
                  index = 0,
                  key = "people/foo",
                  value = Base64.getEncoder
                    .encodeToString("""{"name":"foo"}""".getBytes)
                )
              )
            )
          )
        )
      }
    }
  }

}
