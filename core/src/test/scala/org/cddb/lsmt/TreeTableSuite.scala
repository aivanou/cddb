package org.cddb.lsmt

class TreeTableSuite extends TableSuite with DataConsistency {

  test("treeTable read/insert single value should be successful") {
    val treeTable = TreeTable(defaultMetadata)
    val rec1 = randomRec()
    treeTable.append(rec1)
    assert(treeTable.size == 1)
    val out1 = treeTable.read(rec1.key)
    assert(out1.isDefined)
    assert(out1.get.value.equals(rec1.value))
  }

  test("treeTable read non existent value should produce None") {
    val treeTable = TreeTable(defaultMetadata)
    assert(treeTable.size == 0)
    val out1 = treeTable.read(randomRec().key)
    assert(out1.isEmpty)
  }

  test("treeTable insert with lower timestamp should not update record") {
    val treeTable = TreeTable(defaultMetadata)
    val rec1 = randomRec()
    treeTable.append(rec1)
    val rec2 = Record(rec1.key, "new value", "", rec1.timestamp - 1)
    treeTable.append(rec2)
    val out1 = treeTable.read(rec2.key)
    assert(out1.isDefined)
    assertResult(rec1.key)(out1.get.key)
    assertResult(rec1.value)(out1.get.value)
    assertResult(rec1.timestamp)(out1.get.timestamp)
  }


  test("treeTable update single value should be successful") {
    val treeTable = TreeTable(defaultMetadata)
    val rec1 = randomRec()
    treeTable.append(rec1)
    assert(treeTable.size == 1)
    val rec2 = Record(rec1.key, "new value", "", System.nanoTime())
    treeTable.append(rec2)
    val out1 = treeTable.read(rec2.key)
    assert(out1.isDefined)
    assertResult(rec2.key)(out1.get.key)
    assertResult(rec2.value)(out1.get.value)
    assertResult(rec2.timestamp)(out1.get.timestamp)
  }

  test("treeTable read/insert many unique values") {
    val treeTable = TreeTable(TableMetadata(1000, 0.8, 200))
    val insSize = 500
    val recs = ((0 until insSize) map (_ => randomUniqueRec())).toSet
    for (rec <- recs) treeTable.append(rec)
    for (rec <- recs) {
      val res = treeTable.read(rec.key)
      assert(res.isDefined)
      assertResult(res.get.key)(rec.key)
      assertResult(res.get.value)(rec.value)
    }
  }

  test("TreeTable persist works for the whole table") {
    checkPersisTreeTable(randomRec, TableMetadata(1000, 1.0, 1000), 1000)
  }

  test("TreeTable persist produces relevant SSTable(unique vals)") {
    checkPersisTreeTable(randomRec, TableMetadata(1000, 0.8, 700))
  }

  test("TreeTable persist produces relevant SSTable(non unique vals)") {
    checkPersisTreeTable(simRandomRec, TableMetadata(1000, 0.8, 700))
  }

  def checkPersisTreeTable(recFunc: () => Record, tm: TableMetadata, insSize: Int = 999): Unit = {
    val treeTable = TreeTable(tm)
    val recs = ((0 until insSize) map (_ => recFunc())).toSet
    for (rec <- recs) treeTable.append(rec)
    val tableSize = treeTable.size
    val sstable = treeTable.persistPart
    assertResult(treeTable.size)(tableSize - sstable.size)
    checkSSTableConsistency(sstable)
    for (rec <- sstable.records) {
      val res = treeTable.read(rec.key)
      assert(res.isEmpty)
    }
  }
}
