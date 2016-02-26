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

import org.scalatest._
import collection.JavaConversions._

import scala.collection.immutable.HashMap

/**
  * Created by will on 2/24/16.
  * Testing the basic implementation of docker-java libraries.
  */
class DockerManagerTest extends FlatSpec with BeforeAndAfter {

  private var dm: DockerManager = _
  private var containerId: String = ""

  before {
    //dm =  new DockerManager("https://192.168.99.100:2376")
    dm = new DockerManager()
  }

  after {
    dm.stopContainer(containerId)
  }

  "DockerManager" should "start a container given an image name" in {
    // startContainer is looking for a java.util.Map<Integer, Integer>, so the below converts a scala Map[Int, Int] in java Map<Integer, Integer>
    val exposedPortMap = mapAsJavaMap(Map(7000 -> 7000, 9042 -> 9042, 9160 -> 9160)).asInstanceOf[java.util.Map[Integer, Integer]]
    val envVariables = mapAsJavaMap(Map("CASSANDRA_RPC_ADDRESS" -> "0.0.0.0"))
    containerId = dm.startContainer("cassandra:2.1", exposedPortMap, envVariables)
  }
}
