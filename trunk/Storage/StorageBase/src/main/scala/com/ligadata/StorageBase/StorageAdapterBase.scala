/*
 * This interface is primarily used for implementing storage adapters where
 * row data is partitioned by a time stamp column and also contains one or more key
 * columns
 *
 */
package com.ligadata.StorageBase

import com.ligadata.KamanjaBase.{AdaptersSerializeDeserializers, TransactionContext, ContainerInterface}
import com.ligadata.KvBase.{ Key, TimeRange }
import com.ligadata.Utils.{ KamanjaLoaderInfo }

case class Value(schemaId: Int, serializerType: String, serializedInfo: Array[Byte])

trait DataStoreOperations extends AdaptersSerializeDeserializers {
  // update operations, add & update semantics are different for relational databases
  def put(tnxCtxt: TransactionContext, container: ContainerInterface): Unit = {
    val (outputContainers, serializedContainerData, serializerNames) = serialize(tnxCtxt, Array(container))
    // Call put methods with container name, key & values
  }

  def put(tnxCtxt: TransactionContext, containers: Array[ContainerInterface]): Unit = {
    val (outputContainers, serializedContainerData, serializerNames) = serialize(tnxCtxt, containers)
    // Call put methods with container name, key & values
  }

  // value could be ContainerInterface or Array[Byte]
  def put(tnxCtxt: TransactionContext, containerName: String, key: Key, serializerTyp: String, value: Any): Unit = {
    // Check for value.isInstanceOf[ContainerInterface]
//    val (outputContainers, serializedContainerData, serializerNames) = serialize(tnxCtxt, Array(container))
    // Call put methods with container name, key & values
  }

  // value could be ContainerInterface or Array[Byte]
  def put(tnxCtxt: TransactionContext, containerName: String, keys: Array[Key], serializerTyp: Array[String], values: Array[Any]): Unit = {
    // Check for value.isInstanceOf[ContainerInterface]
//    val (outputContainers, serializedContainerData, serializerNames) = serialize(tnxCtxt, containers)
    // Call put methods with container name, key & values
  }

  // data_list has List of container names, and each container has list of key & value as ContainerInterface or Array[Byte]
  def put(tnxCtxt: TransactionContext, data_list: Array[(String, Array[(Key, String, Any)])]): Unit = {
    // Check for value.isInstanceOf[ContainerInterface]
    //    val (outputContainers, serializedContainerData, serializerNames) = serialize(tnxCtxt, containers)
    // Call put methods with container name, key & values
  }

  // update operations, add & update semantics are different for relational databases
  /* protected */ def put(containerName: String, key: Key, value: Value): Unit
  // def put(containerName: String, data_list: Array[(Key, Value)]): Unit
  /* protected */ def put(data_list: Array[(String, Array[(Key, Value)])]): Unit // data_list has List of container names, and each container has list of key & value

  // delete operations
  def del(containerName: String, keys: Array[Key]): Unit // For the given keys, delete the values
  def del(containerName: String, time: TimeRange, keys: Array[Array[String]]): Unit // For the given multiple bucket key strings, delete the values with in given date range
  def del(containerName: String, time: TimeRange): Unit // For the given date range, delete the values /*Added by Yousef Abu Elbeh at 2016-03-13*/
  // get operations
  // Returns Key, Either[ContainerInterface, Array[Byte]], serializerTyp, TypeName, Version
  def get(containerName: String, callbackFunction: (Key, Any, String, String, Int) => Unit): Unit = {
    val getCallbackFn = (k: Key, v: Value) => {
      if (callbackFunction != null) {
        if (v.schemaId > 0 && v.serializerType != null && v.serializerType.size > 0) {
          val typFromSchemaId = "" // schemaId
          val (cont, deserializerName) = deserialize(v.serializedInfo, v.serializerType)
          callbackFunction(k, cont, v.serializerType, deserializerName, 0)
        } else {
          callbackFunction(k, v.serializedInfo, v.serializerType, null, 0)
        }
      }
    }
    get(containerName, if (callbackFunction != null) getCallbackFn else null)
  }

  // Returns Key, Either[ContainerInterface, Array[Byte]], serializerTyp, TypeName, Version
  def get(containerName: String, keys: Array[Key], callbackFunction: (Key, Any, String, String, Int) => Unit): Unit = {
    val getCallbackFn = (k: Key, v: Value) => {
      if (callbackFunction != null) {
        if (v.schemaId > 0 && v.serializerType != null && v.serializerType.size > 0) {
          val (cont, deserializerName) = deserialize(v.serializedInfo, v.serializerType)
          callbackFunction(k, cont, v.serializerType, deserializerName, 0)
        } else {
          callbackFunction(k, v.serializedInfo, v.serializerType, null, 0)
        }
      }
    }
    get(containerName, keys, getCallbackFn)
  }

  // Returns Key, Either[ContainerInterface, Array[Byte]], serializerTyp, TypeName, Version
  def get(containerName: String, timeRanges: Array[TimeRange], callbackFunction: (Key, Any, String, String, Int) => Unit): Unit  = {
    val getCallbackFn = (k: Key, v: Value) => {
      if (callbackFunction != null) {
        if (v.schemaId > 0 && v.serializerType != null && v.serializerType.size > 0) {
          val (cont, deserializerName) = deserialize(v.serializedInfo, v.serializerType)
          callbackFunction(k, cont, v.serializerType, deserializerName, 0)
        } else {
          callbackFunction(k, v.serializedInfo, v.serializerType, null, 0)
        }
      }
    }
    get(containerName, timeRanges, getCallbackFn)
  }

  // Returns Key, Either[ContainerInterface, Array[Byte]], serializerTyp, TypeName, Version
  def get(containerName: String, timeRanges: Array[TimeRange], bucketKeys: Array[Array[String]], callbackFunction: (Key, Any, String, String, Int) => Unit): Unit  = {
    val getCallbackFn = (k: Key, v: Value) => {
      if (callbackFunction != null) {
        if (v.schemaId > 0 && v.serializerType != null && v.serializerType.size > 0) {
          val (cont, deserializerName) = deserialize(v.serializedInfo, v.serializerType)
          callbackFunction(k, cont, v.serializerType, deserializerName, 0)
        } else {
          callbackFunction(k, v.serializedInfo, v.serializerType, null, 0)
        }
      }
    }
    get(containerName, timeRanges, bucketKeys, getCallbackFn)
  }

  // Returns Key, Either[ContainerInterface, Array[Byte]], serializerTyp, TypeName, Version
  def get(containerName: String, bucketKeys: Array[Array[String]], callbackFunction: (Key, Any, String, String, Int) => Unit): Unit  = {
    val getCallbackFn = (k: Key, v: Value) => {
      if (callbackFunction != null) {
        if (v.schemaId > 0 && v.serializerType != null && v.serializerType.size > 0) {
          val (cont, deserializerName) = deserialize(v.serializedInfo, v.serializerType)
          callbackFunction(k, cont, v.serializerType, deserializerName, 0)
        } else {
          callbackFunction(k, v.serializedInfo, v.serializerType, null, 0)
        }
      }
    }
    get(containerName, bucketKeys, getCallbackFn)
  }

  /* protected */ def get(containerName: String, callbackFunction: (Key, Value) => Unit): Unit
  /* protected */ def get(containerName: String, keys: Array[Key], callbackFunction: (Key, Value) => Unit): Unit
  /* protected */ def get(containerName: String, timeRanges: Array[TimeRange], callbackFunction: (Key, Value) => Unit): Unit // Range of dates
  /* protected */ def get(containerName: String, timeRanges: Array[TimeRange], bucketKeys: Array[Array[String]], callbackFunction: (Key, Value) => Unit): Unit
  /* protected */ def get(containerName: String, bucketKeys: Array[Array[String]], callbackFunction: (Key, Value) => Unit): Unit

  /*
  // Passing filter to storage
  def get(containerName: String, filterFunction: (Key, Value) => Boolean, callbackFunction: (Key, Value) => Unit): Unit
  def get(containerName: String, timeRanges: Array[TimeRange], filterFunction: (Key, Value) => Boolean, callbackFunction: (Key, Value) => Unit): Unit // Range of dates
  def get(containerName: String, timeRanges: Array[TimeRange], bucketKeys: Array[Array[String]], filterFunction: (Key, Value) => Boolean, callbackFunction: (Key, Value) => Unit): Unit
  def get(containerName: String, bucketKeys: Array[Array[String]], filterFunction: (Key, Value) => Boolean, callbackFunction: (Key, Value) => Unit): Unit
*/

  // getKeys operations similar to get, but only key values
  def getKeys(containerName: String, callbackFunction: (Key) => Unit): Unit
  def getKeys(containerName: String, keys: Array[Key], callbackFunction: (Key) => Unit): Unit
  def getKeys(containerName: String, timeRanges: Array[TimeRange], callbackFunction: (Key) => Unit): Unit // Range of dates
  def getKeys(containerName: String, timeRanges: Array[TimeRange], bucketKeys: Array[Array[String]], callbackFunction: (Key) => Unit): Unit
  def getKeys(containerName: String, bucketKeys: Array[Array[String]], callbackFunction: (Key) => Unit): Unit

  def copyContainer(srcContainerName: String, destContainerName: String, forceCopy: Boolean): Unit
  def isContainerExists(containerName: String): Boolean

  def backupContainer(containerName: String): Unit
  def restoreContainer(containerName: String): Unit

  def getAllTables: Array[String] //namespace or schemaName is assumed to be attached to adapter object
  def dropTables(tbls: Array[String]) // here tbls are full qualified name (including namespace)
  def dropTables(tbls: Array[(String, String)])
  def copyTable(srcTableName: String, destTableName: String, forceCopy: Boolean): Unit // here srcTableName & destTableName are full qualified name (including namespace)
  def copyTable(namespace: String, srcTableName: String, destTableName: String, forceCopy: Boolean): Unit
  def isTableExists(tableName: String): Boolean // here tableName is full qualified name (including namespace)
  def isTableExists(tableNamespace: String, tableName: String): Boolean
}

trait DataStore extends DataStoreOperations {
  def beginTx(): Transaction
  def endTx(tx: Transaction): Unit // Same as commit
  def commitTx(tx: Transaction): Unit
  def rollbackTx(tx: Transaction): Unit

  // clean up operations
  def Shutdown(): Unit
  def TruncateContainer(containerNames: Array[String]): Unit
  def DropContainer(containerNames: Array[String]): Unit
  def CreateContainer(containerNames: Array[String]): Unit
}

trait Transaction extends DataStoreOperations {
  val parent: DataStore // Parent Data Store
}

// Storage Adapter Object to create storage adapter
trait StorageAdapterFactory {
  def CreateStorageAdapter(kvManagerLoader: KamanjaLoaderInfo, datastoreConfig: String): DataStore
}
