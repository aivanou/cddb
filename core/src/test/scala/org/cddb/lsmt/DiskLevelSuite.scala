package org.cddb.lsmt


class DiskLevelSuite extends TableSuite with DataConsistency {

  test("dl single table insert should produce index with size=1") {
    val tableManager = SSTableManager()
    val indexHandler = new DataIndexHandler(config)
    val dl = new DiskLevel(config, tableManager, indexHandler)
    dl.scatter(randomSSTable(100, randomRec))
    val ind = indexHandler.readIndex()
    checkDataConsistency(ind)
    assertResult(ind.size)(1)
    val optSSTable = ind.iterator.next._2.load()
    assert(optSSTable.isDefined)
    checkSSTableConsistency(optSSTable.get)
    dl.destroy()
  }

  test("dl single table insert should produce index with size=2 if sstable has more els than maxTableSize") {
    val tableManager = SSTableManager()
    val maxTableSize = 100
    val indexHandler = new DataIndexHandler(config)
    val dl = new DiskLevel(config, tableManager, indexHandler, maxTableSize)
    dl.scatter(randomSSTable(250, randomRec))
    val ind = indexHandler.readIndex()
    checkDataConsistency(ind)
    assertResult(4)(ind.size)
    val it = indexHandler.readIndex().iterator
    while (it.nonEmpty) {
      val (range, db) = it.next
      checkRangeConsistency(range)
      val optTable = db.load()
      assert(optTable.isDefined)
      checkSSTableConsistency(optTable.get)
    }
  }

  test("dl update should not create additional blocks if the size is smaller than MAX_SIZE") {
    val tableManager = SSTableManager()
    val maxTableSize = 1000
    val indexHandler = new DataIndexHandler(config)
    val dl = new DiskLevel(config, tableManager, indexHandler, maxTableSize)
    dl.scatter(randomSSTable(250, randomRec))
    dl.scatter(randomSSTable(250, randomRec))
    dl.scatter(randomSSTable(250, randomRec))
    val ind = indexHandler.readIndex()
    checkDataConsistency(ind)
    assertResult(1)(ind.size)
  }

  test("dl split should occur if records greater than MAX_SIZE") {
    val tableManager = SSTableManager()
    val maxTableSize = 1000
    val indexHandler = new DataIndexHandler(config)
    val dl = new DiskLevel(config, tableManager, indexHandler, maxTableSize)
    dl.scatter(randomSSTable(250, randomRec))
    dl.scatter(randomSSTable(250, randomRec))
    dl.scatter(randomSSTable(250, randomRec))
    dl.scatter(randomSSTable(250, randomRec))
    dl.scatter(randomSSTable(250, randomRec))
    val ind = indexHandler.readIndex()
    checkDataConsistency(ind)
    assertResult(2)(ind.size)
  }

  test("dl scatter append/read operations (not unique vals)") {
    val tableManager = SSTableManager()
    val indexHandler = new DataIndexHandler(config)
    var dl = new DiskLevel(config, tableManager, indexHandler)
    val t1 = randomSSTable(1000, simRandomRec)
    val t2 = randomSSTable(3000, simRandomRec)
    val t3 = randomSSTable(3000, simRandomRec)
    val t4 = randomSSTable(3000, simRandomRec)
    dl.scatter(t1)
    dl.scatter(t2)
    dl.scatter(t3)
    dl.scatter(t4)
    val index = indexHandler.readIndex()
    checkDataConsistency(index)
    dl.cleanAndClose()
    dl = new DiskLevel(config, tableManager, new DataIndexHandler(config))
    checkExistenceOrUpdate(dl, t1)
    checkExistenceOrUpdate(dl, t2)
    checkExistenceOrUpdate(dl, t3)
    checkExistenceOrUpdate(dl, t4)
    dl.destroy()
  }

  def checkExistenceOrUpdate(dl: DiskLevel, table: SSTable): Unit = {
    for (rec <- table.records) {
      val mbRec = dl.read(rec.key)
      assert(mbRec.isDefined)
      val dbRec = mbRec.get
      assert(dbRec.timestamp >= rec.timestamp)
    }
  }

  test("dl scatter append/read operations (unique vals)") {
    val tableManager = SSTableManager()
    val indexHandler = new DataIndexHandler(config)
    var dl = new DiskLevel(config, tableManager, indexHandler)
    val t1 = randomSSTable(1000, randomUniqueRec)
    val t2 = randomSSTable(3000, randomUniqueRec)
    val t3 = randomSSTable(3000, randomUniqueRec)
    val t4 = randomSSTable(3000, randomUniqueRec)
    dl.scatter(t1)
    dl.scatter(t2)
    dl.scatter(t3)
    dl.scatter(t4)
    val index = indexHandler.readIndex()
    checkDataConsistency(index)
    dl.cleanAndClose()
    dl = new DiskLevel(config, tableManager, new DataIndexHandler(config))
    checkExistence(dl, t1)
    checkExistence(dl, t2)
    checkExistence(dl, t3)
    checkExistence(dl, t4)
    dl.destroy()
  }

  def checkExistence(dl: DiskLevel, table: SSTable): Unit = {
    for (rec <- table.records) {
      val mbRec = dl.read(rec.key)
      assert(mbRec.isDefined)
      val dbRec = mbRec.get
      assertResult(rec.value)(dbRec.value)
      assertResult(rec.timestamp)(dbRec.timestamp)
    }
  }


}
