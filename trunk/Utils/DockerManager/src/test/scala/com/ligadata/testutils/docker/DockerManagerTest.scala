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

import java.net.{InetAddress, Inet4Address}

import org.scalatest._

/**
  * Created by will on 2/10/16.
  */
class DockerManagerTest extends FlatSpec with BeforeAndAfter {
  var dm: DockerManager = null
  var id = ""

  before {
    dm = new DockerManager
  }

  after {
    try {
      dm.stopContainer(id)
      dm.removeContainer(id)
    }
    catch {
      case e: Exception => println("Failed to stop or remove the container. Either it wasn't running or it doesn't exist.")
    }
  }

  "Docker Manager" should "instantiate and execute the run method successfully" in {
    id = dm.run("ligadatawilliam/hbase", Some("test-hbase"), List(2181, 9090, 60000), 120)
  }

  it should "return the docker host IP Address" in {
    val ip = dm.getHostIP()
    val expectedIP = new java.net.URI(sys.env("DOCKER_HOST")).getHost
    println(dm.getDockerHostName())
  }
}
