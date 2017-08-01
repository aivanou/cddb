package org.cddb.lsmt

import scala.language.implicitConversions

class SSTableManager() {

  def merge(t1: SSTable, t2: SSTable): SSTable = merge(t1, 0, t1.size, t2, 0, t2.size)

  //TODO: I don't keep track of timestamps and repeated keys and record type(append or delete)
  def merge(t1: SSTable, offset1: Int, nels1: Int, t2: SSTable, offset2: Int, nels2: Int): SSTable = {
    var t1Ind = 0
    var t2Ind = 0
    var recInd = 0
    val records = new Array[Record](nels1 + nels2)
    while (t1Ind < nels1 && t2Ind < nels2) {
      val (rec1, rec2) = (t1.get(offset1 + t1Ind), t2.get(offset2 + t2Ind))
      val cmp = rec1.key.compareTo(rec2.key)
      if (cmp == 0) {
        records(recInd) = resolutionStrategy(rec1, rec2)
        t1Ind += 1
        t2Ind += 1
      }
      else if (cmp > 0) {
        records(recInd) = rec2
        t2Ind += 1
      } else {
        records(recInd) = rec1
        t1Ind += 1
      }
      recInd += 1
    }
    while (t1Ind < nels1) {
      records(recInd) = t1.get(offset1 + t1Ind)
      t1Ind += 1
      recInd += 1
    }
    while (t2Ind < nels2) {
      records(recInd) = t2.get(offset2 + t2Ind)
      t2Ind += 1
      recInd += 1
    }
    SSTable.withSize(records, recInd)
  }

  def resolutionStrategy(r1: Record, r2: Record): Record = {
    if (r1.timestamp > r2.timestamp) r1
    else r2
  }

  def split(table: SSTable): (SSTable, SSTable) = {
    table.splitEqual()
  }

}


object SSTableManager {

  def apply(): SSTableManager = new SSTableManager()

}


