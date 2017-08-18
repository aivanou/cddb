package org.cddb.lsmt

import org.cddb.io.{Config, DiskBlock}

import scala.collection.immutable

/**
  *
  * @param config
  */
class DataIndexHandler(config: Config) {

  implicit def ord = new Ordering[Range] {
    override def compare(x: Range, y: Range): Int = {
      val cmp = x.from.compareTo(y.from)
      if (cmp == 0) {
        val cmp1 = x.to.compareTo(y.to)
        if (cmp1 == 0) {
          java.lang.Long.compare(x.creationTimestamp, y.creationTimestamp)
        } else cmp1
      } else cmp
    }
  }

  private val indexBlock = DiskBlock[DataIndex](config, DataIndex, config.dataIndex)

  def readIndex(): immutable.TreeMap[Range, DiskBlock[DiskSSTable]] = {
    indexBlock.readFromDisk() match {
      case None => immutable.TreeMap[Range, DiskBlock[DiskSSTable]]()(ord)
      case Some(data) => parse(data)
    }
  }

  def writeIndex(tree: immutable.TreeMap[Range, DiskBlock[DiskSSTable]]): Unit = {
    val lst = tree.map {
      case (range, block) => Metadata(range, block.getId)
    }
    val dataIndex = new DataIndex(lst.toList)
    indexBlock.writeToDisk(dataIndex)
  }

  def destroy(): Unit = {
    indexBlock.destroy()
    //    val dir = new File(config.storagePath)
    //    dir.deleteOnExit()
  }

  def cleanAndClose(): Unit = {
    indexBlock.cleanAndClose()
  }

  private def parse(dataIndex: DataIndex): immutable.TreeMap[Range, DiskBlock[DiskSSTable]] = {
    def process(dt: List[Metadata], mp: immutable.TreeMap[Range, DiskBlock[DiskSSTable]]): immutable.TreeMap[Range, DiskBlock[DiskSSTable]] =
      dt match {
        case h :: t => mp
        case Nil => mp
      }
    process(dataIndex.indices, immutable.TreeMap[Range, DiskBlock[DiskSSTable]]()(ord))
  }

}
