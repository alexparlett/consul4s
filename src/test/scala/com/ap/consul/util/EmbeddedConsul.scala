package com.ap.consul.util

import org.testcontainers.consul.ConsulContainer
import org.testcontainers.utility.DockerImageName

trait EmbeddedConsul {

  def withRunningConsulOnFoundPort[T](body: ConsulContainer => T): T = {

    val container = new ConsulContainer(DockerImageName.parse("consul:1.14.4"))
    container.start()

    try
      body(container)
    finally
      container.stop()

  }

  def getConsulAddress(implicit consulContainer: ConsulContainer): String = {
    s"http://${consulContainer.getHost}:${consulContainer.getFirstMappedPort}"
  }
}
