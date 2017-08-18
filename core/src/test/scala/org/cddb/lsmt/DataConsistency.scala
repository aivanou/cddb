package org.cddb.lsmt

import org.cddb.io.DiskBlock
import org.scalatest.FunSuite


trait DataConsistency extends FunSuite {

  def checkSSTableOrder(low: SSTable, high: SSTable): Unit = {
    checkNonOverlapping(low, high)
    compareRanges(getRange(low), getRange(high))
  }

  def checkNonOverlapping(t1: SSTable, t2: SSTable): Unit = {
    checkSSTableConsistency(t1)
    checkSSTableConsistency(t2)
    checkNonOverlapping(getRange(t1), getRange(t2))
  }

  def checkDataConsistency(ind: scala.collection.immutable.TreeMap[Range, DiskBlock[DiskSSTable]]): Unit = {
    val it = ind.iterator
    if (it.isEmpty) return
    var prev = it.next
    checkSSTableConsistency(prev._2.load())
    while (it.nonEmpty) {
      val (currRange, currBlock) = it.next()
      currBlock.load() match {
        case Some(table) =>
          checkSSTableConsistency(table)
        case None =>
      }
      val prevRange = prev._1
      compareRanges(prevRange, currRange)
      prev = (currRange, currBlock)
    }
  }

  def checkRangeList(ranges: List[Range]): Unit = {
    for (i <- 1 until ranges.size) {
      compareRanges(ranges(i - 1), ranges(i))
    }
  }

  def checkNonOverlapping(r1: Range, r2: Range): Unit = {
    assert(!isOverlapping(r1, r2))
  }

  def getRange(table: SSTable): Range = {
    val r1 = table.tryRead(0).get.key
    val rn = table.tryRead(table.getSize - 1).get.key
    Range(r1, rn, System.nanoTime())
  }

  def isOverlapping(r1: Range, r2: Range): Boolean = {
    r1.from.compareTo(r2.to) <= 0 && r1.to.compareTo(r2.from) >= 0
  }

  def compareRanges(low: Range, high: Range): Unit = {
    assert(checkRangeConsistency(low))
    assert(checkRangeConsistency(high))
    assert(high.from.compareTo(low.to) > 0)
  }

  def checkRangeConsistency(r: Range): Boolean = r.to.compareTo(r.from) >= 0

  def checkSSTableConsistency(table: Option[DiskSSTable]): Unit = table match {
    case Some(t) => checkSSTableConsistency(t)
    case None =>
  }

  def checkSSTableConsistency(table: SSTable): Unit = {
    if (table.getSize == 0) return
    for (i <- 1 until table.getSize) {
      (table.tryRead(i - 1), table.tryRead(i)) match {
        case (Some(prev), Some(curr)) =>
          t(prev, curr)
          assert(prev.key.compareTo(curr.key) < 0)
        case (None, None) =>
        case (Some(_), None) => assert(false)
        case (None, Some(_)) => assert(false)
      }
    }
  }

  def t(r1: Record, r2: Record): Unit = {
    if (r1.key.compareTo(r2.key) >= 0) {
      val k = 0
    }
  }

}
