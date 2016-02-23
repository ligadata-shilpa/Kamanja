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

package com.ligadata.InputOutputAdapterInfo

import com.ligadata.HeartBeat.MonitorComponentInfo
import org.scalatest._

/**
  * Created by will on 2/9/16.
  */

private class MockOutputAdapter extends OutputAdapter {
  var testMessages: Seq[String] = Seq()
  var testPartKeys: Seq[String] = Seq()

  override val inputConfig: AdapterConfiguration = null

  override def Shutdown: Unit = ???

  // To send an array of messages. messages.size should be same as partKeys.size
  override def send(messages: Array[Array[Byte]], partKeys: Array[Array[Byte]]): Unit = {
    messages.foreach(msg => {
      testMessages = testMessages :+ new String(msg)
    })

    partKeys.foreach(key => {
      testPartKeys = testPartKeys :+ new String(key)
    })
  }

  override def getComponentStatusAndMetrics: MonitorComponentInfo = ???
}

class OutputAdapterTests extends FlatSpec with BeforeAndAfter with Matchers {
  private var outputAdapter: MockOutputAdapter = null

  before {
    outputAdapter = new MockOutputAdapter
  }

  "OutputAdapter" should "send a single message and a single partition key" in {
    outputAdapter.send("This is a message", "This is a partition key")
    outputAdapter.testMessages should contain("This is a message")
    outputAdapter.testPartKeys should contain("This is a partition key")
  }

  it should "send a String Array of messages and partition keys" in {
    val msgArr: Array[String] = Array("Message 1", "Message 2", "Message 3")
    val partKeyArr: Array[String] = Array("Partition Key 1", "Partition Key 2")
    outputAdapter.send(msgArr, partKeyArr)
    outputAdapter.testMessages should contain("Message 1")
    outputAdapter.testMessages should contain("Message 2")
    outputAdapter.testMessages should contain("Message 3")
    outputAdapter.testPartKeys should contain("Partition Key 1")
    outputAdapter.testPartKeys should contain("Partition Key 2")
  }

  it should "send a Byte Array of a single message and partition key" in {
    outputAdapter.send("This is a message".getBytes("UTF8"), "This is a partition key".getBytes("UTF8"))
    outputAdapter.testMessages should contain("This is a message")
    outputAdapter.testPartKeys should contain("This is a partition key")
  }

  it should "be instantiated with Category set at Output" in {
    assert(outputAdapter.Category == "Output")
  }
}
