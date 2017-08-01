package org.cddb.lsmt

import org.cddb.io.Config

case class TableMetadata(maxSize: Int, softThreshold: Double, persistSize: Int)

case class Result()

case class Record(key: String, value: String, status: String, timestamp: Long)

case class IndexRecord(key: String, offset: Long)

case class Index(records: List[IndexRecord])


class LevelHandler(memoryLevel: MemoryLevel, diskLevel: DiskLevel) {

  def append(rec: Record): Result = {
    memoryLevel.append(rec)
  }

  def read(key: String): Option[Record] = {
    memoryLevel.read(key) match {
      case Some(ans) => Some(ans)
      case None => diskLevel.read(key)
    }
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

object LSMT {
  def main(args: Array[String]): Unit = {

  }

}
