package com.ligadata.keyvaluestore.mapdb

import org.mapdb._;
import com.ligadata.keyvaluestore._
import java.io.File;
import java.nio.ByteBuffer

/*

No schema setup

 */

class KeyValueHashMapTx(owner: DataStore) extends Transaction {
  var parent: DataStore = owner

  def add(source: IStorage) = { owner.add(source) }
  def put(source: IStorage) = { owner.put(source) }
  def get(key: Key, target: IStorage) = { owner.get(key, target) }
  def get(key: Key, handler: (Value) => Unit) = { owner.get(key, handler) }
  def del(key: Key) = { owner.del(key) }
  def del(source: IStorage) = { owner.del(source) }
  def getAllKeys(handler: (Key) => Unit) = { owner.getAllKeys(handler) }
}

class KeyValueHashMap(parameter: PropertyMap) extends DataStore {
  var path = parameter.getOrElse("path", ".")
  var keyspace = parameter.getOrElse("schema", "default")
  var table = parameter.getOrElse("table", "default")

  val InMemory = parameter.getOrElse("inmemory", "false").toBoolean
  val withTransactions = parameter.getOrElse("withtransaction", "false").toBoolean

  var db: DB = null

  if (InMemory == true) {
    db = DBMaker.newMemoryDB()
      .make()
  } else {
    db = DBMaker.newFileDB(new File(path + "/" + keyspace + ".hdb"))
      .closeOnJvmShutdown()
      .asyncWriteEnable()
      .asyncWriteFlushDelay(100)
      .mmapFileEnable()
      .transactionDisable()
      .commitFileSyncDisable()
      .make()
  }

  var map = db.createHashMap(table)
    .hasher(Hasher.BYTE_ARRAY)
    .makeOrGet[Array[Byte], Array[Byte]]()

  def add(source: IStorage) =
    {
      map.putIfAbsent(source.Key.toArray[Byte], source.Value.toArray[Byte])
      if (withTransactions)
        db.commit() //persist changes into disk
    }

  def put(source: IStorage) =
    {
      map.put(source.Key.toArray[Byte], source.Value.toArray[Byte])
      if (withTransactions)
        db.commit() //persist changes into disk
    }

  def get(key: Key, handler: (Value) => Unit) =
    {
      val buffer = map.get(key.toArray[Byte])

      // Construct the output value
      val value = new Value
      if (buffer != null) {
        value ++= buffer
      } else {
        throw new KeyNotFoundException("Key Not found")
      }

      handler(value)
    }

  def get(key: Key, target: IStorage) =
    {
      val buffer = map.get(key.toArray[Byte])

      // Construct the output value
      val value = new Value
      if (buffer != null) {
        value ++= buffer
      } else {
        throw new KeyNotFoundException("Key Not found")
      }

      target.Construct(key, value)
    }

  def del(key: Key) =
    {
      map.remove(key.toArray[Byte])
      if (withTransactions)
        db.commit(); //persist changes into disk
    }

  def del(source: IStorage) = { del(source.Key) }

  def beginTx(): Transaction = { new KeyValueHashMapTx(this) }

  def endTx(tx: Transaction) = {}

  def commitTx(tx: Transaction) = {}

  override def Shutdown() =
    {
      db.commit(); //persist changes into disk
      map.close();
    }

  def TruncateStore() {
    map.clear()
    if (withTransactions)
      db.commit() //persist changes into disk

    // Defrag on startup
    db.compact()
  }

  def getAllKeys(handler: (Key) => Unit) =
    {
      var iter = map.keySet().iterator()
      while (iter.hasNext()) {
        val buffer = iter.next()

        // Construct the output value
        // BUGBUG-jh-20140703: There should be a more concise way to get the data
        //
        val key = new Key
        for (b <- buffer)
          key += b

        handler(key)
      }
    }
}

