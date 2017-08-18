package org.cddb.lsmt

import java.io.File
import java.nio.ByteBuffer

import org.cddb.io.FileManager
import org.cddb.lsmt.internal.TableSerializer

import scala.annotation.tailrec
import scala.collection._

case class IndexRecord(key: String, offset: Int, size: Int)

class DiskSSTable(fm: FileManager) extends SSTable {

  import DiskSSTable._

  private var index: Option[immutable.TreeSet[IndexRecord]] = None
  private var recordsOffset: Option[Int] = None
  private var records: Option[Array[Record]] = None

  def getIndex: immutable.TreeSet[IndexRecord] = {
    index = index match {
      case None => loadIndex()
      case _ => index
    }
    index.get
  }

  def getRecords: Array[Record] = {
    records = records match {
      case None => loadSSTable()
      case Some(_) => records
    }
    records.get
  }

  override def getSize: Int = getIndex.size

  override def tryRead(key: String): Option[Record] = {
    val indRecs = getIndex
    val offset = recordsOffset.get
    indRecs.find(rec => rec.key.equals(key)) match {
      case Some(indRec) =>
        val bytes = fm.read(offset + indRec.offset, indRec.size)._1
        Some(TableSerializer.Record.parseFrom(bytes.array()))
      case None => None
    }
  }

  override def destroy(): Unit = {
    cleanAndClose()
    fm.destroy()
  }

  override def cleanAndClose(): Unit = {
    index = None
    records = None
    fm.close()
  }

  private def loadIndex(): Option[immutable.TreeSet[IndexRecord]] = {
    val osize = fm.readInt(0).get
    val (bytes, bread) = fm.read(osize)
    if (bread != osize) {
      return None
    }
    recordsOffset = Some(bytes.limit() + 4)
    val records = TableSerializer.SSTableIndex.parseFrom(bytes.array())
    Some(toCollection(records, 0, immutable.TreeSet[IndexRecord]()(indOrd)))
  }

  private def loadSSTable(): Option[Array[Record]] = {
    val indRecs = getIndex
    val records = new Array[Record](indRecs.size)
    fm.setPosition(recordsOffset.get)
    indRecs.toStream.zipWithIndex.foreach {
      case (indRec, ind) =>
        val (bytes, size) = fm.read(indRec.size)
        val record = TableSerializer.Record.parseFrom(bytes)
        records(ind) = record
    }
    Some(records)
  }

  @tailrec
  private def toCollection(records: TableSerializer.SSTableIndex, pos: Int,
                           out: immutable.TreeSet[IndexRecord]): immutable.TreeSet[IndexRecord] =
    if (pos == records.getRecordsCount) {
      out
    } else {
      toCollection(records, pos + 1, out + records.getRecords(pos))
    }
}

object DiskSSTable {

  def apply(path: String, blockId: String) = ???

  private def persist(mtable: SSTable, path: String, blockId: String): DiskSSTable = {
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val file = new File(path + "/" + blockId)
    if (!file.exists()) {
      file.createNewFile()
    }
    val recs = toBytes(mtable)
    val indBytes = buildIndex(recs)
    val fm = new FileManager(path + "/" + blockId)
    fm.setPosition(0)
    fm.writeInt(indBytes.length)
    fm.write(ByteBuffer.wrap(indBytes))
    for ((_, bytes) <- recs) {
      fm.write(ByteBuffer.wrap(bytes))
    }
    fm.flush()
    fm.setPosition(0)
    new DiskSSTable(fm)
  }

  private def buildIndex(recs: Array[(String, Array[Byte])]): Array[Byte] = {
    val bldr = TableSerializer.SSTableIndex.newBuilder()
    var offset = 0
    for ((key, bytes) <- recs) {
      bldr.addRecords(IndexRecord(key, offset, bytes.length))
      offset += bytes.length
    }
    bldr.build().toByteArray
  }

  private def toBytes(mtable: SSTable): Array[(String, Array[Byte])] = {
    val res = new Array[(String, Array[Byte])](mtable.getSize)
    for (i <- 0 until mtable.getSize) {
      mtable.tryRead(i) match {
        case Some(rec) => res(i) = (rec.key, rec.toByteArray)
        case None => //TODO: throw exception
      }
    }
    res
  }

  implicit def recOrd = new Ordering[Record]() {
    override def compare(x: Record, y: Record): Int = x.key.compareTo(y.key)
  }

  implicit def indOrd = new Ordering[IndexRecord]() {
    override def compare(x: IndexRecord, y: IndexRecord): Int = x.key.compareTo(y.key)
  }

  implicit def toProtoSSTableIndex(ind: immutable.TreeSet[IndexRecord]): TableSerializer.SSTableIndex = {
    val bldr = TableSerializer.SSTableIndex.newBuilder()
    for (rec <- ind) {
      bldr.addRecords(rec)
    }
    bldr.build()
  }

  implicit def toProtoIndexRecord(indRec: IndexRecord): TableSerializer.Pointer =
    TableSerializer.Pointer.newBuilder()
      .setKey(indRec.key)
      .setOffset(indRec.offset)
      .setLength(indRec.size)
      .build()

  implicit def toIndexRecord(ptr: TableSerializer.Pointer): IndexRecord =
    IndexRecord(ptr.getKey(), ptr.getOffset(), ptr.getLength())

  implicit def toProtoRec(rec: Record): org.cddb.lsmt.internal.TableSerializer.Record =
    TableSerializer.Record.newBuilder()
      .setKey(rec.key)
      .setValue(rec.value)
      .setStatus(rec.status)
      .setTimestamp(rec.timestamp).build()

  implicit def toRecord(rec: TableSerializer.Record): Record =
    Record(rec.getKey(), rec.getValue(), rec.getStatus(), rec.getTimestamp())

}