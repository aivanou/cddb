package org.cddb.lsmt

import java.nio.ByteBuffer
import java.util.UUID

import org.cddb.io.{Config, FileManager}
import org.cddb.lsmt.internal.TableSerializer

import scala.collection.immutable.Iterable
import scala.collection.{immutable, _}

case class Range(from: String, to: String, creationTimestamp: Long)

class DiskLevel(config: Config, indexHandler: DataIndexHandler, maxSSTableSize: Int = 1000) {

  private var ranges = immutable.TreeMap[Range, SSTable[Record]]()(rangeOrd)

  def tryRead(key: String): Option[Record] = {
    None
  }

  def scatter(tables: Array[SSTable[Record]]): Unit = {
    val tablesToMerge = findOverlappingTables(getRanges(tables))

  }

  implicit def rangeOrd = new Ordering[Range]() {
    override def compare(x: Range, y: Range): Int = x.from.compareTo(y.from)
  }

  def findOverlappingTables(ranges: Array[Range]): Set[(Range, SSTable[Record])] = {
    def find(i: Int): Set[(Range, SSTable[Record])] = {
      if (i >= ranges.length) return Set()
      find(i + 1) ++ findOverlappingTables(ranges(i))
    }
    find(0)
  }

  def findOverlappingTables(range: Range): Iterable[(Range, SSTable[Record])] = {
    ranges filter {
      case ((tableRange, sstable)) => isOverlapping(range, tableRange)
    }
  }

  def getRanges(tables: Array[SSTable[Record]]): Array[Range] = {
    val ranges = new Array[Range](tables.length)
    for (i <- tables.indices) {
      ranges(i) = tables(i).getRange
    }
    ranges
  }

  def isOverlapping(r1: Range, r2: Range): Boolean = {
    if (r1.from.compareTo(r2.to) > 0 || r1.to.compareTo(r2.from) < 0) {
      false
    } else {
      true
    }
  }

  def hash(record: Record): Int = record.hashCode()

}

object DiskLevel {

  def apply(config: Config, tableManager: SSTableManager, indexHandler: DataIndexHandler, maxSize: Int = 500): DiskLevel =
    new DiskLevel(config, indexHandler, maxSize)

  def nwayMerge(tables: Array[SSTable[Record]],
                inTables: Array[SSTable[Record]],
                maxSSTableSize: Int = 1000): Unit = {
    val iterators = new Array[Iterator[Record]](tables.length + inTables.length)
    val pq = mutable.PriorityQueue[(Record, Int)]()(pqOrd)
    for (i <- tables.indices) {
      iterators(i) = tables(i).iterator
    }
    for (i <- inTables.indices) {
      iterators(i + tables.length) = inTables(i).iterator
    }
    for (ind <- iterators.indices) {
      if (iterators(ind).hasNext) {
        pq += ((iterators(ind).next(), ind))
      }
    }
    var mtable = new MutableSSTable()
    while (pq.nonEmpty) {
      val (topRec, tableInd) = pq.dequeue()
      updatePQ(iterators, tableInd, pq)
      var recToAdd = topRec
      while (pq.nonEmpty && pq.head._1.key.equals(topRec.key)) {
        val (secRec, tind) = pq.dequeue()
        recToAdd = resolve(recToAdd, secRec)
        updatePQ(iterators, tind, pq)
      }
      mtable.add(recToAdd)
      if (mtable.getSize == maxSSTableSize) {
        val blockId = UUID.randomUUID().toString
        mtable.persist(path)
        mtable = new MutableSSTable()
      }
    }
    if (mtable.getSize > 0) {
      //persist
    }
  }

  def updatePQ(iterators: Array[Iterator[Record]],
               ind: Int, pq: mutable.PriorityQueue[(Record, Int)]): Unit = {
    if (iterators(ind).isEmpty) return
    val record = iterators(ind).next()
    pq += ((record, ind))
  }

  def resolve(r1: Record, r2: Record): Record = {
    if (r1.timestamp > r2.timestamp) r1 else r2
  }

  implicit def pqOrd = new Ordering[(Record, Int)]() {
    override def compare(x: (Record, Int), y: (Record, Int)): Int = {
      val cmp = x._1.key.compareTo(y._1.key)
      if (cmp == 0) -1 * x._1.timestamp.compareTo(y._1.timestamp)
      else -cmp
    }
  }

}

class MutableSSTable {

  import DiskSSTable._

  private var offest = 0

  private val indSerializer = TableSerializer.SSTableIndex.newBuilder()
  private val serializedRecords = mutable.ListBuffer[ByteBuffer]()

  def getSize: Int = serializedRecords.size

  def add(rec: Record): Unit = {
    val recBytes = rec.toByteArray
    indSerializer.addRecords(IndexRecord(rec.key, offest, recBytes.length))
    serializedRecords += ByteBuffer.wrap(recBytes)
    offest += recBytes.length
  }

  def persist(path: String, blockId: String): Unit = {
    FileManager.create(path, blockId)
    val fm = new FileManager(path + "/" + blockId)
    fm.setPosition(0)
    fm.write(ByteBuffer.wrap(indSerializer.build().toByteArray))
    serializedRecords.foreach(fm.write)
    fm.close()
  }

}
