package org.cddb.lsmt

import org.cddb.Timer
import org.cddb.io.Config


class DiskLevelSuite extends TableSuite with DataConsistency {

  override val config: Config = Config("/Users/aliaksandrivanou/code/cddb/temp", "ind.dat", TableMetadata(500))

  test("Merging of overlapping tables should produce a set of non overlapping tables") {
    var dts = Array[SSTable](randomDiskSSTable(genRecords(randomRec, 500).toArray))
    var mts = Array[SSTable](randomMemSSTable(500, randomRec))
    testMerge(mts, dts)
    for (table <- dts) table.destroy()
    dts = randomDiskSSTables(50, 1000, randomRec)
    mts = randomMemSSTables(20, 1000, randomRec)
    testMerge(mts, dts)
    for (table <- dts) table.destroy()
    dts = randomDiskSSTables(50, 1000, randomUniqueRec)
    mts = randomMemSSTables(20, 1000, randomUniqueRec)
    testUniqueMerge(mts, dts)
    for (table <- dts) table.destroy()
    dts = Array[SSTable]()
    mts = randomMemSSTables(20, 1000, randomUniqueRec)
    testUniqueMerge(mts, dts)
    for (table <- dts) table.destroy()

  }

  test("Merge tables should be fast ") {
    var dts = randomDiskSSTables(10, 1000000, randomRec)
    var mts = randomMemSSTables(4, 1000000, randomRec)
    val msize = 1000000
    println("stopped generating")
    Timer.start("nway_merge")
    val resTables = DiskLevel.nwayMerge(mts, dts, msize)
    Timer.stop("nway_merge")
    println(Timer.provide("nway_merge"))
    for (i <- 1 until resTables.length) {
      checkSSTableOrder(resTables(i - 1), resTables(i))
    }
    for (table <- dts) table.destroy()
  }

  test("Merge tables should reduce amount of tables ") {
    var dts = Array[SSTable]()
    var mts = randomMemSSTables(20, 1000, randomRec)
    val msize = 20000
    val resTables = DiskLevel.nwayMerge(mts, dts, msize)
    for (i <- 1 until resTables.length) {
      checkSSTableOrder(resTables(i - 1), resTables(i))
    }
    assertResult(1)(resTables.length)
  }

  def testUniqueMerge(a1: Array[SSTable], a2: Array[SSTable], msize: Int = 1000) = {
    val resTables = DiskLevel.nwayMerge(a1, a2, msize)
    for (i <- 1 until resTables.length) {
      checkSSTableOrder(resTables(i - 1), resTables(i))
    }
    assertResult(a1.length + a2.length)(resTables.length)
  }

  def testMerge(a1: Array[SSTable], a2: Array[SSTable], msize: Int = 1000) = {
    val resTables = DiskLevel.nwayMerge(a1, a2, msize)
    for (i <- 1 until resTables.length) {
      checkSSTableOrder(resTables(i - 1), resTables(i))
    }
  }

}
