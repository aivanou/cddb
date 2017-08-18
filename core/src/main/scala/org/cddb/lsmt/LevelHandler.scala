package org.cddb.lsmt

import org.cddb.io.Config

case class TableMetadata(maxSize: Int)

case class Result()

case class Record(key: String, value: String, status: String, timestamp: Long)

class LevelHandler(memoryLevel: MemoryLevel, diskLevel: DiskLevel) {

  def append(rec: Record): Result = {
    memoryLevel.append(rec)
  }

  def read(key: String): Option[Record] = {
    None
  }

  def cleanAndClose(): Unit = {
  }

  def destroy(): Unit = {
  }

}

object LevelHandler {

  def apply(config: Config): LevelHandler = {
    //    val config = Config("/Users/aliaksandrivanou/temp/data", "index.dat", TableMetadata(500, 0.4, 300))
    val sstableManager = SSTableManager()
    val indexHandler = new DataIndexHandler(config)
    val diskLevel = DiskLevel(config, sstableManager, indexHandler)
    val memoryLevel = MemoryLevel(config, diskLevel)
    new LevelHandler(memoryLevel, diskLevel)
  }

}