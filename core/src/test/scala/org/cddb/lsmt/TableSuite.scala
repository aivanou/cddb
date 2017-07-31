package org.cddb.lsmt

import java.util.UUID

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TableSuite extends FunSuite {

  val metadata = TableMetadata(100, 0.8, 20)

  test("treeTable read/insert single value should not produce errors") {
    val treeTable = TreeTable(metadata)
    val rec1 = randomRec()
    treeTable.append(rec1)
    assert(treeTable.size == 1)
    val out1 = treeTable.read(rec1.key)
    assert(out1.isInstanceOf[Some[Record]])
    assert(out1.get.value.equals(rec1.value))
  }

  test("treeTable read/insert many values") {
    val treeTable = TreeTable(metadata)

  }

  test("treeTable read/insert to SSTable") {
    val treeTable = TreeTable(metadata)
    (0 until 50).foreach(_ => treeTable.append(randomRec()))
    assert(treeTable.size == 50)
    val sstable = treeTable.persistPart
    assert(treeTable.size == 30)
    assert(sstable.size == 20)
    val tm = SSTableManager()
    val outSStable1 = SSTable.deserialize(SSTable.serialize(sstable))
    assert(outSStable1.get.size == 20)
  }

  test("SSTable split should produce two tables") {
    val mn = SSTableManager()
    val sstable = randomSSTable(101)
    val (t1, t2) = mn.split(sstable)
    assert(t1.size == sstable.size / 2)
    assert(t2.size == (sstable.size - sstable.size / 2))
    for (ind <- 0 until t1.size) {
      assert(sstable.get(ind) == t1.get(ind))
    }
    for (ind <- 0 until t2.size) {
      assert(sstable.get(t1.size + ind) == t2.get(ind))
    }
  }

  test("SSTable merge should merge two tables") {
    val mn = SSTableManager()
    val t1 = randomSSTable(101)
    val t2 = randomSSTable(201)
    val res = mn.merge(t1, t2)
    assert(res.size == t1.size + t2.size)
    for (ind <- 1 until res.size) {
      val (crec, prec) = (res.get(ind), res.get(ind - 1))
      assert(crec.key.compareTo(prec.key) >= 0)
    }
  }

  def randomSSTable(size: Int) = randomTreeTable(size).persistPart

  def randomTreeTable(size: Int) = {
    val tt = TreeTable(TableMetadata(size, 1.0, size))
    (0 until size).foreach(_ => tt.append(randomRec()))
    tt
  }

  def randomRec(): Record = Record(rstring(), rstring(), rstring(), System.nanoTime())

  def rstring(): String = UUID.randomUUID().toString

}
