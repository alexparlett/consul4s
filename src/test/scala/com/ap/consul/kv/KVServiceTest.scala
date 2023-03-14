package com.ap.consul.kv

import java.util.Base64

import cats.effect.IO
import com.ap.consul.kv.KV.{DeleteParams, GetParams}
import com.ap.consul.kv.KVServiceTest.Person
import com.ap.consul.util.EmbeddedConsul
import io.circe.generic.AutoDerivation
import io.circe.generic.auto._
import org.http4s.client.{Client, JavaNetClientBuilder}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.junit.JUnitRunner
import org.testcontainers.consul.ConsulContainer

object KVServiceTest extends AutoDerivation {
  case class Person(name: String)
}

@RunWith(classOf[JUnitRunner])
class KVServiceTest extends FunSuite with Matchers with EmbeddedConsul {

  private implicit val client: Client[IO] = JavaNetClientBuilder[IO].create

  test("testSet") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new KVService(getConsulAddress)

      val success = underTest.set("people/foo", Person("foo"))

      success shouldBe true

      val result =
        actualConfig.execInContainer("consul", "kv", "get", "people/foo")

      result.getStdout
        .replaceAll("\\r?\\n", "") shouldBe """{"name":"foo"}"""
    }
  }

  test("testGet") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new KVService(getConsulAddress)

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/foo",
        """{"name":"foo"}"""
      )

      val result = underTest.get("people/foo")

      result should not be (null)
      Base64.getDecoder.decode(result.head.value) should be(
        """{"name":"foo"}""".getBytes
      )

      result.head.asOpt[Person] should contain(Person("foo"))
    }
  }

  test("testGet recurse") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new KVService(getConsulAddress)

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/foo",
        """{"name":"foo"}"""
      )

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/bar",
        """{"name":"bar"}"""
      )

      val result = underTest.get("people", GetParams(recurse = Some(true)))

      result should not be (null)
      result should have size (2)

      Base64.getDecoder.decode(
        result.filter(_.key.equals("people/foo")).head.value
      ) should be("""{"name":"foo"}""".getBytes)

      result.filter(_.key.equals("people/foo")).head.asOpt[Person] should contain(
        Person("foo")
      )

      Base64.getDecoder.decode(
        result.filter(_.key.equals("people/bar")).head.value
      ) should be("""{"name":"bar"}""".getBytes)

      result.filter(_.key.equals("people/bar")).head.asOpt[Person] should contain(
        Person("bar")
      )
    }
  }

  test("testGetRaw") {

    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new KVService(getConsulAddress)

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/foo",
        """{"name":"foo"}"""
      )

      val result = underTest.raw[Person]("people/foo")

      result should not be (null)
      result.name should be("foo")
    }
  }

  test("testKeys") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new KVService(getConsulAddress)

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/foo",
        """{"name":"foo"}"""
      )

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/bar",
        """{"name":"bar"}"""
      )

      val result = underTest.keys("people")

      result should not be (null)
      result should have size (2)
      result should contain only ("people/bar", "people/foo")
    }
  }

  test("testDelete") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new KVService(getConsulAddress)

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/foo",
        """{"name":"foo"}"""
      )

      val result = underTest.delete("people/foo")

      result should be(true)
    }
  }

  test("testDelete recurse") {
    withRunningConsulOnFoundPort { implicit actualConfig: ConsulContainer ⇒
      val underTest = new KVService(getConsulAddress)

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/foo",
        """{"name":"foo"}"""
      )

      actualConfig.execInContainer(
        "consul",
        "kv",
        "put",
        "people/bar",
        """{"name":"bar"}"""
      )

      val result =
        underTest.delete("people", DeleteParams(recurse = Some(true)))

      result should be(true)
    }
  }
}
