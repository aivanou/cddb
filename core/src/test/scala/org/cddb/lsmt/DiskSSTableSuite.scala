package org.cddb.lsmt

import java.util.UUID

class DiskSSTableSuite extends TableSuite with DataConsistency {

  test("Disk SSTable should be correctly written to the disk") {
    val recs = genRecords(randomRec, 1000)
    val dt = randomDiskSSTable(recs)
    dt.destroy()
  }

  test("Disk SSTable should be fast") {
    val recs = genRecords(randomRec, 1000000)
    val dt = randomDiskSSTable(recs)
    dt.destroy()
  }


  test("Disk SSTable should be consistent") {
    val recs = genRecords(randomRec, 1000)
    val dt = randomDiskSSTable(recs.toArray)
    for (rec <- recs) {
      val diskRec = dt.tryRead(rec.key)
      assert(diskRec.isDefined)
      assertResult(rec.key)(diskRec.get.key)
      assert(diskRec.get.timestamp >= rec.timestamp)
      if (diskRec.get.timestamp == rec.timestamp) {
        assertResult(rec.value)(diskRec.get.value)
      }
    }
    dt.destroy()
  }

  test("Reading non existent value should return NONE") {
    val dt = randomDiskSSTable(genRecords(randomRec, 10).toArray)
    val r1 = dt.tryRead(UUID.randomUUID().toString)
    assert(r1.isEmpty)
    val r2 = dt.tryRead(5000)
    assert(r2.isEmpty)
    for (i <- 0 until 10) {
      assert(dt.tryRead(i).isDefined)
    }
    dt.destroy()
  }

  test("Reading value should not interfere with reading the whole table") {
    val recs = genRecords(randomRec, 1000).toArray
    val dt = randomDiskSSTable(recs)
    for (i <- 0 until 100) {
      assert(dt.tryRead(i).isDefined)
    }
    assert(dt.getSize > 0)
    assert(dt.getSize <= recs.length)
    assertContains(recs, dt)
    dt.destroy()
  }

  def assertContains(recs: Array[Record], table: SSTable): Unit = {
    for (rec <- recs) {
      val res = table.tryRead(rec.key)
      assert(res.isDefined)
      checkRecordsEqual(rec, res.get)
    }
  }

  def checkRecordsEqual(low: Record, high: Record): Unit = {
    assertResult(low.key)(high.key)
    assert(low.timestamp <= high.timestamp)
    if (low.timestamp == high.timestamp) {
      assertResult(low.value)(high.value)
    }
  }
}
