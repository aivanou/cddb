package org.cddb.lsmt

import java.io.File
import java.util.UUID

import org.cddb.io.Config
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
abstract class TableSuite extends FunSuite
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  val storageDir = "./.temp/data"

  val config = Config(storageDir, "index.dat", TableMetadata(2000, 0.7, 1500))


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val dir = new File(storageDir)
    if (dir.exists()) {
      val tableManager = SSTableManager()
      val indexHandler = new DataIndexHandler(config)
      val dl = new DiskLevel(config, tableManager, indexHandler)
      dl.destroy()
      dir.delete()
    }
    dir.mkdirs()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    val dir = new File(storageDir)
    if (dir.exists()) {
      val tableManager = SSTableManager()
      val indexHandler = new DataIndexHandler(config)
      val dl = new DiskLevel(config, tableManager, indexHandler)
      dl.destroy()
      dir.delete()
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val dir = new File(storageDir)
    if (dir.exists()) {
      val tableManager = SSTableManager()
      val indexHandler = new DataIndexHandler(config)
      val dl = new DiskLevel(config, tableManager, indexHandler)
      dl.destroy()
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
      val dl = new DiskLevel(config, tableManager, indexHandler)
      dl.destroy()
      dir.delete()
    }
  }


  val defaultMetadata = TableMetadata(100, 0.8, 20)

  val rng = new java.util.Random(System.nanoTime())

  val stableRng = new java.util.Random(100)

  def randomSSTable(size: Int, recFunc: () => Record) = randomTreeTable(size, recFunc).persistPart

  def randomTreeTable(size: Int, recFunc: () => Record) = {
    val tt = TreeTable(TableMetadata(size, 1.0, size))
    (0 until size).foreach(_ => tt.append(recFunc()))
    tt
  }

  def randomRec(): Record = genRec(rstring, 100000)

  def simRandomRec(): Record = genRec(rstring, 100)

  def randomUniqueRec(): Record = genRec(ustring, 0)

  def genRec(rstring: (Int) => String, msize: Int): Record =
    Record(rstring(msize), rstring(msize), rstring(msize), System.nanoTime())

  def ustring(msize: Int): String = UUID.randomUUID().toString

  def rstring(msize: Int): String = rng.nextInt(msize).toString

}
