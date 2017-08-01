package org.cddb.lsmt

import scala.collection.immutable

class TreeTable(metadata: TableMetadata) extends Table {

  private var data = immutable.TreeMap[String, Record]()

  def append(newRecord: Record): Result = {
    data = data.get(newRecord.key) match {
      case Some(tableRec) =>
        data + ((tableRec.key, resolve(tableRec, newRecord)))
      case None =>
        data + ((newRecord.key, newRecord))
    }
    Result()
  }

  private def resolve(tableRecord: Record, newRecord: Record): Record = {
    if (tableRecord.timestamp > newRecord.timestamp) tableRecord
    else newRecord
  }

  def read(key: String): Option[Record] = {
    data.get(key)
  }

  def size: Int = data.size

  def persistPart: SSTable = {
    //lock

    def gather(): Array[Record] = {
      val persistSize = Math.min(metadata.persistSize, data.size)
      val arr = new Array[Record](persistSize)
      for (i <- 0 until persistSize) {
        val rec = data.head._2
        data = data - rec.key
        arr(i) = rec
      }
      arr
    }
    SSTable(gather())

    //unlock
  }

}

object TreeTable {

  def apply(metadata: TableMetadata): TreeTable = new TreeTable(metadata)

}