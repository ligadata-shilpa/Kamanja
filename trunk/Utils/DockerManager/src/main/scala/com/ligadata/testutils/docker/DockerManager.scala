/*
 * Copyright 2016 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.testutils.docker

import java.net.InetAddress

import se.marcuslonnberg.scaladocker.remote.api._
import se.marcuslonnberg.scaladocker.remote.models._
import se.marcuslonnberg.scaladocker.remote.models.Port.Tcp
import akka.actor._
import akka.stream._

import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import sys.process._

/**
  * Created by will on 2/10/16.
  */

object DockerLogger {
  val logger: org.apache.logging.log4j.Logger = org.apache.logging.log4j.LogManager.getLogger(this.getClass)
}

trait DockerLogger {
  val logger = DockerLogger.logger
}

class DockerManager extends DockerLogger {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))
  val client = new DockerClient(DockerConnection.fromEnvironment())

  def run(imageName: String, timeout: Int = 30): String = {
    run(imageName, None, List(), timeout)
  }

  def run(imageName: String, containerName: Option[String], portsToBind: List[Int], timeout: Int): String = {
    val image = ImageName(imageName)
    logger.info(s"Pulling Image $imageName. This may take several minutes...")
    val pullResult = Await.result(client.pullFuture(image), timeout seconds)
    logger.info(s"Successfully pulled $imageName")

    val cName: Option[ContainerName] = containerName match {
      case Some(name) => Some(ContainerName(name))
      case None => None
    }

    val portBindings: Map[Port, Seq[PortBinding]] = portsToBind.map(port => (Tcp(port), Seq(PortBinding("0.0.0.0", port)))).toMap

    val container = Await.result(client.create(ContainerConfig(image)), timeout seconds)
    val id = container.id

    try {
      if (!portBindings.isEmpty) {
        val hostConfig = HostConfig(portBindings = portBindings)
        Await.result(client.start(id, Some(hostConfig)), timeout seconds).toString
      }
      else {
        Await.result(client.start(id, None), timeout seconds).toString
      }
    }
    catch {
      case e: UnknownResponseException => throw new Exception("DOCKER-MANAGER: Failed to start container with id " + id, e)
      case e: Exception => throw new Exception("DOCKER-MANAGER: Failed to start container with id " + id, e)
    }
  }

  def stopContainer(containerId: String, timeout: Int = 30): Unit = {
    try {
      Await.result(client.stop(ContainerHashId(containerId)), timeout seconds)
    }
    catch {
      case e: Exception => throw new Exception(s"Failed to stop container with id $containerId", e)
    }
  }

  def removeContainer(containerId: String, timeout: Int = 30): Unit = {
    try {
      val containerSubStr = containerId.toString.substring(0, 11)
      Await.result(client.rm(ContainerHashId(containerId)), timeout seconds)
    }
    catch {
      case e: Exception => throw new Exception(s"Failed to remove container with id $containerId", e)
    }
  }

  def getHostIP(timeout: Int = 30): String = {
    return new java.net.URI(DockerConnection.fromEnvironment().baseUri.toString).getHost
  }

  def getDockerHostName(timeout: Int = 30): String = {
    val addr: InetAddress = InetAddress.getByName(getHostIP(timeout))
    val host: String = addr.getHostName()
    return host
  }
}
