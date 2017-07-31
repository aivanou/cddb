package org.cddb.lsmt

import scala.collection.immutable

class TreeTable(metadata: TableMetadata) extends Table {

  private var data = immutable.TreeMap[String, Record]()

  def append(record: Record): Result = {
    data = data + ((record.key, record))
    Result()
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