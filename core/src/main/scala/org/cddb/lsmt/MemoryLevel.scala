package org.cddb.lsmt

import org.cddb.io.Config

class MemoryLevel(config: Config, innerLevel: DiskLevel) {

  private val treeTable: TreeTable = TreeTable(config.tm)


  def append(record: Record): Result = {
    treeTable.append(record)
    if (passedThreshold()) {
      propagate(treeTable.persistPart)
    }
    Result()
  }

  def propagate(table: SSTable): Unit = {
    innerLevel.scatter(table)
  }

  def passedThreshold(): Boolean = treeTable.size >= config.tm.maxSize * config.tm.softThreshold

  def read(key: String): Option[Record] = treeTable.read(key)

}

object MemoryLevel {
  def apply(config: Config, innerLevel: DiskLevel): MemoryLevel = new MemoryLevel(config, innerLevel)
}
