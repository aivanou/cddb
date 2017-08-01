package org.cddb.lsmt

import org.cddb.io.DiskBlock


trait DataConsistency {

  def checkDataConsistency(ind: scala.collection.immutable.TreeMap[Range, DiskBlock[SSTable]]): Unit = {
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

  def compareRanges(low: Range, high: Range): Unit = {
    assert(checkRangeConsistency(low))
    assert(checkRangeConsistency(high))
    assert(high.from.compareTo(low.to) > 0)
  }

  def checkRangeConsistency(r: Range): Boolean = r.to.compareTo(r.from) >= 0

  def checkSSTableConsistency(table: Option[SSTable]): Unit = table match {
    case Some(t) => checkSSTableConsistency(t)
    case None =>
  }

  def checkSSTableConsistency(table: SSTable): Unit = {
    if (table.size == 0) return
    for (i <- 1 until table.size) {
      val (prev, curr) = (table.get(i - 1), table.get(i))
      assert(curr.key.compareTo(prev.key) > 0)
    }
  }

  //  def checkTreeTableConsistency(table: TreeTable): Unit = {
  //    val it = table.resolve()
  //  }

}
