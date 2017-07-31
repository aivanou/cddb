package org.cddb.lsmt

class MemoryLevel(innerLevel: DiskLevel, hf: String => Long) {

  private val treeTable: TreeTable = TreeTable(TableMetadata(500, 0.4, 300))


  def append(record: Record): Result = {
    treeTable.append(record)

  }

  def read(key: String): Option[Record] = treeTable.read(key)

}

object MemoryLevel {
  def main(args: Array[String]): Unit = {
  }
}
