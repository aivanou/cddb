package org.cddb.lsmt

import java.io.File
import java.util.UUID

import org.cddb.Timer
import org.cddb.io.Config
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

@RunWith(classOf[JUnitRunner])
abstract class TableSuite extends FunSuite
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  val storageDir = "./.temp/data"

  val config = Config(storageDir, "index.dat", TableMetadata(2000))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val dir = new File(storageDir)
    if (dir.exists()) {
      val tableManager = SSTableManager()
      val indexHandler = new DataIndexHandler(config)
      //      val dl = new DiskLevel(config, tableManager, indexHandler)
      //      dl.destroy()
      //      dir.delete()
    }
    dir.mkdirs()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    val dir = new File(storageDir)
    if (dir.exists()) {
      val tableManager = SSTableManager()
      val indexHandler = new DataIndexHandler(config)
      //      val dl = new DiskLevel(config, tableManager, indexHandler)
      //      dl.destroy()
      dir.delete()
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val dir = new File(storageDir)
    if (dir.exists()) {
      val tableManager = SSTableManager()
      val indexHandler = new DataIndexHandler(config)
      //      val dl = new DiskLevel(config, tableManager, indexHandler)
      //      dl.destroy()
      dir.delete()
    }
    dir.mkdirs()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    val dir = new File(storageDir)
    if (dir.exists()) {
      val tableManager = SSTableManager()
      val indexHandler = new DataIndexHandler(config)
      //      val dl = new DiskLevel(config, tableManager, indexHandler)
      //      dl.destroy()
      dir.delete()
    }
  }


  val defaultMetadata = TableMetadata(100)

  val rng = new java.util.Random(System.nanoTime())

  val stableRng = new java.util.Random(100)

  def randomMemSSTables(numTables: Int, tableSize: Int, genfunc: () => Record): Array[SSTable] = {
    val res = new Array[SSTable](numTables)
    (0 until numTables) foreach (ind => res(ind) = randomMemSSTable(tableSize, genfunc))
    res
  }

  def randomDiskSSTables(numTables: Int, tableSize: Int, genfunc: () => Record): Array[SSTable] = {
    val res = new Array[SSTable](numTables)
    (0 until numTables) foreach (ind => res(ind) = randomDiskSSTable(genRecords(genfunc, tableSize)))
    res
  }

  def randomDiskSSTable(recs: Array[Record]): SSTable = {
    var a = treeTable(recs)
    val mt = a.persist
    val path = "/Users/aliaksandrivanou/code/cddb/temp"
    val bid = UUID.randomUUID().toString
    Timer.start("111")
    val res = DiskSSTable(mt, path, bid)
    Timer.stop("111")
    res
  }

  def assertContains(checkValues: Array[Record], data: Array[Record]): Unit = {
    checkValues.foreach(rec => assertContains(data, rec))
  }

  def assertContains(data: Array[Record], rec: Record): Unit = {
    assertResult(1)(data.count(dr => dr.key.equals(rec.key)))
  }

  def randomMemSSTable(size: Int, recFunc: () => Record) = randomTreeTable(size, recFunc).persist

  def treeTable(recs: Array[Record]): TreeTable = {
    val tt = TreeTable(TableMetadata(recs.length))
    for (i <- recs.indices) {
      tt.append(recs(i))
    }
    tt
  }

  def randomTreeTable(size: Int, recFunc: () => Record) = {
    val tt = TreeTable(TableMetadata(size))
    (0 until size).foreach(_ => tt.append(recFunc()))
    tt
  }

  def genRecords(gfn: () => Record = randomRec, size: Int = 200): Array[Record] = {
    val res = new Array[Record](size)
    for (i <- 0 until size) {
      res(i) = gfn()
    }
    res
  }

  def randomRec(): Record = genRec(rstring, 100000000)

  def simRandomRec(): Record = genRec(rstring, 100)

  def randomUniqueRec(): Record = genRec(ustring, 0)

  def genRec(rstring: (Int) => String, msize: Int): Record =
    Record(rstring(msize), rstring(msize), rstring(msize), System.nanoTime())

  def ustring(msize: Int): String = UUID.randomUUID().toString

  def rstring(msize: Int): String = rng.nextInt(msize).toString

}
