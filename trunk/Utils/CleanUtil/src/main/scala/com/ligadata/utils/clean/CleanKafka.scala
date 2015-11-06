/*
 * Copyright 2015 ligaDATA
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

package com.ligadata.utils.clean

import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import scala.concurrent.duration._

object CleanKafka {
  val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  def deleteTopic(topicName: String, zookeeperConnectString: String): Unit = {
    logger.info(s"CLEAN-UTIL: Deleting kafka topic '$topicName'")
    val zkClient: ZkClient = new ZkClient(zookeeperConnectString, 30000, 30000, ZKStringSerializer)
    if(AdminUtils.topicExists(zkClient, topicName))
      AdminUtils.deleteTopic(zkClient, topicName)
    else {
      logger.warn(s"CLEAN-UTIL: Kafka topic '$topicName' does not exist. Skipping delete...")
      return
    }

    val timeout = 5.seconds.fromNow
    while(timeout.hasTimeLeft()) {
      if (!AdminUtils.topicExists(zkClient, topicName)) {
        logger.info(s"CLEAN-UTIL: Successfully deleted topic '$topicName'")
        return
      }
    }
    logger.error(s"CLEAN-UTIL: Failed to delete kafka topic '$topicName'\n" +
      s"Please ensure you have Kafka 0.8.2.0 or later and your kafka configuration has 'delete.topic.enable' set to 'true'")
  }
}