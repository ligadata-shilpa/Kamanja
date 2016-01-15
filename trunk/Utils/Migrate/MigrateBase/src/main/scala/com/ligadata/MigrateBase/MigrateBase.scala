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

package com.ligadata.MigrateBase

trait MigratableFrom {
  def init(srouceInstallPath: String, metadataStoreInfo: String, dataStoreInfo: String, statusStoreInfo: String): Unit
  def isInitialized: Boolean
  def getAllMetadataTableNames: Array[String]
  def getAllDataTableNames: Array[String]
  def getAllStatusTableNames: Array[String]
  def getAllMetadataDataStatusTableNames: Array[String]
  def getAllMetadataObjs(backupTblSufix: String, callbackFunction: (String, String) => Boolean): Unit // Callback function calls with metadata Object Type & metadata information in JSON string
  def getAllDataObjs(backupTblSufix: String, metadataElemsJson: Array[(String, String)], callbackFunction: (Array[(String, Long, Array[String], Long, Int, String, String)]) => Boolean): Unit // Callback function calls with list of (container name, timepartition value, bucketkey, transactionid, rowid, serializername & data in Gson (JSON) format).
  def shutdown: Unit
}

trait MigratableTo {
  def init(apiConfigFile: String, clusterConfigFile: String): Unit
  def isInitialized: Boolean
  def getMetadataStoreInfo: String
  def getDataStoreInfo: String
  def getStatusStoreInfo: String
  def getMetadataStoreDataStoreStatusStoreInfo: (String, String, String) // the return configuration information is in the order of Metadata Store Info, Data Store Info & Status Store Info
  def isTableExists(tblName: String): Boolean
  def backupTables(tblsToBackedUp: Array[(String, String)], force: Boolean): Unit
  def dropTables(tblsToBackedUp: Array[String]): Unit
  def uploadConfiguration: Unit // Uploads clusterConfigFile file
  def addMetadata(metadataElemsJson: Array[(String, String)]): Unit // Tuple has metadata element type & metadata element data in JSON format.
  def populateAndSaveData(data: Array[(String, Long, Array[String], Long, Int, String, String)]): Unit // Array of tuples has container name, timepartition value, bucketkey, transactionid, rowid, serializername & data in Gson (JSON) format
  def shutdown: Unit
}

