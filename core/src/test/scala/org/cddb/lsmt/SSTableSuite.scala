package org.cddb.lsmt

class SSTableSuite extends TableSuite with DataConsistency {

  test("SSTable split should produce two tables(unique vals)") {
    //    val mn = SSTableManager()
    //    val sstable = randomSSTable(101, randomUniqueRec)
    //    val (t1, t2) = mn.split(sstable)
    //    assert(t1.size == sstable.size / 2)
    //    assert(t2.size == (sstable.size - sstable.size / 2))
    //    for (ind <- 0 until t1.size) {
    //      assert(sstable.get(ind) == t1.get(ind))
    //    }
    //    for (ind <- 0 until t2.size) {
    //      assert(sstable.get(t1.size + ind) == t2.get(ind))
    //    }
    //    checkSSTableConsistency(t1)
    //    checkSSTableConsistency(t2)
  }

  test("SSTable merge should merge two tables(unique values)") {
    //    val mn = SSTableManager()
    //    val t1 = randomSSTable(101, randomUniqueRec)
    //    val t2 = randomSSTable(201, randomUniqueRec)
    //    val res = mn.merge(t1, t2)
    //    assert(res.size == t1.size + t2.size)
    //    checkSSTableConsistency(res)
    //    for (rec <- t1.records) {
    //      val mrec = res.read(rec.key)
    //      assert(mrec.isDefined)
    //      assert(mrec.get.key.equals(rec.key))
    //      assert(mrec.get.value.equals(rec.value))
    //      assert(mrec.get.timestamp == rec.timestamp)
    //    }
    //    for (rec <- t2.records) {
    //      val mrec = res.read(rec.key)
    //      assert(mrec.isDefined)
    //      assert(mrec.get.key.equals(rec.key))
    //      assert(mrec.get.value.equals(rec.value))
    //      assert(mrec.get.timestamp == rec.timestamp)
    //    }
  }

  test("SSTable merge should merge two tables(not unique values)") {
    //    val mn = SSTableManager()
    //    val t1 = randomSSTable(101, simRandomRec)
    //    val t2 = randomSSTable(201, simRandomRec)
    //    val res = mn.merge(t1, t2)
    //    assert(res.size <= t1.size + t2.size)
    //    checkSSTableConsistency(res)
    //    for (ind <- 0 until res.size) {
    //      val rec = res.get(ind)
    //      val t1Out = t1.read(rec.key)
    //      val t2Out = t2.read(rec.key)
    //      assert(t1Out.isDefined || t2Out.isDefined)
    //    }
  }

  test("SSTable read non existent value should produce None") {
    //    val treeTable = TreeTable(TableMetadata(500, 1.0, 500))
    //    val recs = ListBuffer[Record]()
    //    (0 until 500) foreach { ind =>
    //      val rec = Record(ind.toString, ind.toString, ind.toString, System.nanoTime())
    //      recs += rec
    //      treeTable.append(rec)
    //    }
    //    val sstable = treeTable.persistPart
    //    checkSSTableConsistency(sstable)
    //    (500 until 1000) foreach { ind =>
    //      val res = sstable.read(ind.toString)
    //      assert(res.isEmpty)
    //    }
  }

  test("SSTable recursive split") {
    //    val sstable = randomSSTable(1000, randomRec)
    //    val tables = recursiveSplit(sstable, SSTableManager(), 100)
    //    tables.foreach(t => checkSSTableConsistency(t))
    //    checkRangeList(tables.map(getRange))
  }

  //  def getRange(table: DiskSSTable, l: Int): Range = Range(table.get(l).key, table.get(table.size - 1).key, System.nanoTime())
  //
  //  def getRange(table: DiskSSTable): Range = Range(table.get(0).key, table.get(table.size - 1).key, System.nanoTime())
  //
  //
  //  def recursiveSplit(table: DiskSSTable, mngr: SSTableManager, maxSize: Int): List[DiskSSTable] = {
  //    if (table.size <= maxSize) List(table)
  //    else {
  //      val (ltable, rtable) = mngr.split(table)
  //      recursiveSplit(ltable, mngr, maxSize) ::: recursiveSplit(rtable, mngr, maxSize)
  //    }
  //  }
  //
  //  test("SSTable serialization/deserialization should be successful") {
  //    val sstable = randomSSTable(1000, randomRec)
  //    val tsize = sstable.size
  //    val tm = SSTableManager()
  //    val outSStable1 = DiskSSTable.deserialize(DiskSSTable.serialize(sstable))
  //    assert(outSStable1.isDefined)
  //    assertResult(outSStable1.get.size)(tsize)
  //    checkSSTableConsistency(outSStable1)
  //  }
  //
  //  test("SSTable serialization/deserialization of empty talbe should be successful") {
  //    val sstable = randomSSTable(0, randomRec)
  //    val tsize = sstable.size
  //    val tm = SSTableManager()
  //    val outSStable1 = DiskSSTable.deserialize(DiskSSTable.serialize(sstable))
  //    assert(outSStable1.isDefined)
  //    assertResult(outSStable1.get.size)(tsize)
  //    checkSSTableConsistency(outSStable1)
  //  }

}
