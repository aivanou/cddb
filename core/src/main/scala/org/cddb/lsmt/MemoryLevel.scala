package org.cddb.lsmt

import java.util.concurrent.atomic.AtomicReference

import org.cddb.io.Config

import scala.annotation.tailrec

/**
  * Contains functionality for working with memory sstables.
  *
  * @param config
  * @param innerLevel
  */
class MemoryLevel(config: Config, innerLevel: DiskLevel) {

  private val maxMemTables = 4
  private var memTableIndex = 0

  private val treeTable: AtomicReference[TreeTable] = new AtomicReference[TreeTable]()
  private val memTables = new Array[SSTable](maxMemTables)

  def append(record: Record): Result = {
    val tt = treeTable.get()
    tt.append(record)
    if (tt.isFull) {
      treeTable.set(new TreeTable(config.tm))
      persist(tt)
    }
    Result()
  }

  def persist(table: TreeTable): Unit = {
    val memSSTable = table.persist
    memTables(memTableIndex) = memSSTable
    memTableIndex += 1
    if (memTableIndex == maxMemTables) {
      val scatterTables = new Array[SSTable](maxMemTables)
      for (i <- 0 until maxMemTables) {
        scatterTables(i) = memTables(i)
      }
      memTableIndex = 0
      innerLevel.scatter(scatterTables)
    }
  }

  def read(key: String): Option[Record] = {
    val tt = treeTable.get()
    tt.read(key) match {
      case None => findInMemTables(key)
      case Some(rec) => Some(rec)
    }
  }

  private def findInMemTables(key: String): Option[Record] = {
    @tailrec
    def find(ind: Int): Option[Record] =
      if (ind >= memTableIndex) None
      else {
        findInMemTable(memTables(ind), key) match {
          case None => find(ind + 1)
          case Some(rec) => Some(rec)
        }
      }
    find(0)
  }

  private def findInMemTable(table: SSTable, key: String): Option[Record] = table.tryRead(key)

}

object MemoryLevel {
  def apply(config: Config, innerLevel: DiskLevel): MemoryLevel = new MemoryLevel(config, innerLevel)
}
