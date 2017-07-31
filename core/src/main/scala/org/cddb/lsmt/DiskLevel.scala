package org.cddb.lsmt

import java.util.UUID

import org.cddb.io.{Config, DiskBlock}

import scala.collection._

case class Range(from: String, to: String, creationTimestamp: Long)

class DiskLevel(tableManager: SSTableManager, indexHandler: DataIndexHandler) {

  val config = Config("/Users/aliaksandrivanou/temp/data", "index.dat")

  val maxSSTableSize = 1000

  private var ranges = indexHandler.readIndex()

  /**
    *
    * @param table
    */
  def scatter(table: SSTable): Unit = {
    var lInd = 0
    val cleanupBlocks = mutable.ListBuffer[(Range, DiskBlock[SSTable])]()
    val newTables = mutable.ListBuffer[(Range, SSTable)]()
    val it = ranges.iterator
    if (it.isEmpty) {
      newTables += ((getRange(table), table))
    } else {
      while (it.nonEmpty && lInd < table.size) {
        val (range, block) = it.next
        val rInd =
          if (it.isEmpty) Some(table.size - 1)
          else checkNextRange(range, block, table, lInd)
        rInd match {
          case Some(ind) =>
            cleanupBlocks += ((range, block))
            val (newRange, newSSTable) = merge(block, range, table, lInd, ind)
            if (newSSTable.size >= maxSSTableSize) {
              val ((lrange, ltable), (rrange, rtable)) = split(newRange, newSSTable)
              newTables += ((lrange, ltable))
              newTables += ((rrange, rtable))
            } else {
              newTables += ((newRange, newSSTable))
            }
            lInd = ind + 1
          case None =>
        }

      }
    }
    for ((range, table) <- newTables) {
      val newBlock = DiskBlock(config, SSTable, UUID.randomUUID().toString)
      newBlock.save(table)
      ranges = ranges + ((range, newBlock))
    }
    for ((range, block) <- cleanupBlocks) {
      ranges = ranges - range
      block.destroy()
    }
    indexHandler.writeIndex(ranges)
  }

  def cleanAndClose(): Unit = {
    for ((range, block) <- ranges) {
      block.cleanAndClose()
    }
    indexHandler.cleanAndClose()
  }

  def destroy(): Unit = {
    for ((range, block) <- ranges) {
      block.destroy()
    }
    indexHandler.destroy()
  }

  def split(range: Range, table: SSTable): ((Range, SSTable), (Range, SSTable)) = {
    val (ltable, rtable) = tableManager.split(table)
    ((getRange(ltable), ltable), (getRange(rtable), rtable))
  }

  def checkNextRange(range: Range, block: DiskBlock[SSTable], table: SSTable, lInd: Int): Option[Int] = {
    if (range.from.compareTo(table.get(table.size - 1).key) >= 0) {
      Some(table.size - 1)
    } else if (isOverlap(range, getRange(table, lInd))) {
      val rInd = findRightIndex(range.to, table, lInd)
      Some(rInd)
    } else {
      None
    }
  }

  def findFirstOverlappingIndex(r: Range, sstable: SSTable, lInd: Int): Int = {
    for (i <- lInd until sstable.size) {
      val rec = sstable.get(i)
      if (r.from.compareTo(rec.key) <= 0) {
        return i
      }
    }
    sstable.size
  }

  def merge(block: DiskBlock[SSTable], range: Range, table: SSTable, l: Int, r: Int): (Range, SSTable) = {
    val innerSSTable = block.load().get
    val newSSTable = tableManager.merge(innerSSTable, 0, innerSSTable.size, table, l, r - l + 1)
    (getRange(newSSTable), newSSTable)
  }

  def findRightIndex(key: String, table: SSTable, leftInd: Int): Int = {
    var l = leftInd
    var r = table.size - 1
    while (l <= r) {
      val mid = l + (r - l) / 2
      if (table.get(mid).key.compareTo(key) > 0) {
        r = mid - 1
      } else {
        l = mid + 1
      }
    }
    r
  }

  def isOverlap(r: Range, sstable: SSTable, l: Int): Boolean = isOverlap(r, sstable, l, sstable.size - 1)

  def isOverlap(r: Range, sstable: SSTable): Boolean = isOverlap(r, sstable, 0, sstable.size - 1)

  def isOverlap(r1: Range, sstable: SSTable, l: Int, r: Int): Boolean = {
    val (leftKey, rightKey) = (sstable.get(l).key, sstable.get(r).key)
    isOverlap(r1, Range(leftKey, rightKey, System.nanoTime()))
  }

  def isOverlap(r1: Range, r2: Range): Boolean = {
    if (r1.from.compareTo(r2.to) > 0) {
      return false
    } else if (r1.to.compareTo(r2.from) < 0) {
      return false
    }
    return true
  }

  def getRange(table: SSTable, l: Int): Range = Range(table.get(l).key, table.get(table.size - 1).key, System.nanoTime())

  def getRange(table: SSTable): Range = Range(table.get(0).key, table.get(table.size - 1).key, System.nanoTime())

  def hash(record: Record): Int = record.hashCode()

}

object DiskLevel {

  def apply(tableManager: SSTableManager, indexHandler: DataIndexHandler): DiskLevel =
    new DiskLevel(tableManager, indexHandler)

}
