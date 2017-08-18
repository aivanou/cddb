package org.cddb.lsmt

import scala.collection.immutable

class MemorySSTable(private val records: immutable.TreeMap[String, Record]) extends SSTable[Record] {

  def getSize: Int = records.size

  private val range = Range(records.firstKey, records.lastKey, System.nanoTime())

  override def tryRead(key: String): Option[Record] = {
    records.get(key)
  }

  override def destroy(): Unit = {}

  override def cleanAndClose(): Unit = {}

  override def getRange: Range = range

  override def iterator: Iterator[Record] = records.values.iterator
}

object MemorySSTable {

  def apply(recs: immutable.TreeMap[String, Record]): MemorySSTable = new MemorySSTable(recs)

}
