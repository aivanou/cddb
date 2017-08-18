package org.cddb.lsmt

import scala.collection._

/**
  * The data that is stored in memory.(first level)
  *
  * @param metadata
  */
class TreeTable(metadata: TableMetadata) extends Table {

  private var data = immutable.TreeMap[String, Record]()

  def append(newRecord: Record): Result = {
    data = data.get(newRecord.key) match {
      case Some(tableRecord) => data + ((newRecord.key, resolve(tableRecord, newRecord)))
      case None => data + ((newRecord.key, newRecord))
    }
    Result()
  }

  def isFull: Boolean = data.size == metadata.maxSize

  private def resolve(tableRecord: Record, newRecord: Record): Record = {
    if (tableRecord.timestamp > newRecord.timestamp) tableRecord
    else newRecord
  }

  def read(key: String): Option[Record] = {
    val res = data.get(key)
    Option(res)
  }

  def size: Int = data.size

  def persist: SSTable = {
    val recs = data
    data = immutable.TreeMap[String, Record]()
    new MemorySSTable(recs)
  }

}

object TreeTable {

  def apply(metadata: TableMetadata): TreeTable = new TreeTable(metadata)

}