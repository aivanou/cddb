package org.cddb.lsmt

import java.util.Random

import org.cddb.io.{Config, DiskBlock}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class DiskLevelSuite extends FunSuite {

  val config = Config("/Users/aliaksandrivanou/temp/data", "index.dat")

  val rand = new Random(System.nanoTime())

  test("dl scatter test") {
    val tableManager = SSTableManager()
    val indexHandler = new DataIndexHandler(config)
    var dl = new DiskLevel(tableManager, indexHandler)
    val t1 = randomSSTable(500)
    val t2 = randomSSTable(300)
    val t3 = randomSSTable(300)
    val t4 = randomSSTable(300)
    dl.scatter(t1)
    dl.scatter(t2)
    dl.scatter(t3)
    dl.scatter(t4)
    dl.scatter(randomSSTable(1000))
    dl.scatter(randomSSTable(1000))
    dl.scatter(randomSSTable(1000))
    dl.scatter(randomSSTable(1000))
    dl.scatter(randomSSTable(1000))
    dl.scatter(randomSSTable(1000))
    dl.scatter(randomSSTable(1000))
    dl.scatter(randomSSTable(10000))

    val index = indexHandler.readIndex()
    checkDataConsistency(index)
    dl.cleanAndClose()
    dl.destroy()
  }

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

  def randomSSTable(size: Int) = randomTreeTable(size).persistPart

  def randomTreeTable(size: Int) = {
    val tt = TreeTable(TableMetadata(size, 1.0, size))
    (0 until size).foreach(_ => tt.append(randomRec()))
    tt
  }

  def randomRec(): Record = Record(rstring(), rstring(), rstring(), System.nanoTime())

  def rstring(): String = rand.nextInt(2000).toString

}
