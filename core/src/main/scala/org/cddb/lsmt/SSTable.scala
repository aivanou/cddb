package org.cddb.lsmt

import com.google.protobuf.InvalidProtocolBufferException
import org.cddb.lsmt.internal.TableSerializer

import scala.collection.mutable

class SSTable(private[lsmt] val records: Array[Record], offset: Int, nels: Int) extends Table {

  import SSTable._

  private[lsmt] val index = init()

  private def init(): mutable.HashMap[String, Int] = {
    val intMap = mutable.HashMap[String, Int]()
    //TODO check that records.size not greater than nels
    for (ind <- offset until (offset + nels)) {
      intMap.put(records(ind).key, ind)
    }
    intMap
  }

  def size: Int = nels

  def get(ind: Int): Record = records(offset + ind)

  def findIndex(key: String): Option[Int] = index.get(key)

  def splitEqual(): (SSTable, SSTable) = {
    (SSTable.partial(records, offset, nels / 2), SSTable.partial(records, offset + nels / 2, nels - nels / 2))
  }

  def read(key: String): Option[Record] = {
    findIndex(key) match {
      case Some(index) => Some(records(index))
      case _ => None
    }
  }

  def serialize(): Array[Byte] = {
    val tableBuilder = TableSerializer.SSTable.newBuilder()
    for (ind <- offset until (offset + nels)) {
      tableBuilder.addRecords(records(ind))
    }
    tableBuilder.build().toByteArray
  }

}

object SSTable extends Serializer[SSTable] {

  implicit def toProtoRec(rec: Record): org.cddb.lsmt.internal.TableSerializer.Record =
    TableSerializer.Record.newBuilder()
      .setKey(rec.key)
      .setValue(rec.value)
      .setStatus(rec.status)
      .setTimestamp(rec.timestamp).build()

  implicit def toRecord(rec: TableSerializer.Record): Record =
    Record(rec.getKey(), rec.getValue(), rec.getStatus(), rec.getTimestamp())

  def apply(records: Array[Record]) = {
    new SSTable(records, 0, records.length)
  }

  def withSize(records: Array[Record], size: Int) = {
    new SSTable(records, 0, size)
  }

  def partial(records: Array[Record], offset: Int, nels: Int) = new SSTable(records, offset, nels)

  def merge(t1: SSTable, t2: SSTable): SSTable = null

  def split(table: SSTable, splitKey: String): (SSTable, SSTable) = null

  override def serialize(table: SSTable): Array[Byte] = table.serialize()

  override def deserialize(data: Array[Byte]): Option[SSTable] = {
    try {
      val internalSSTable = TableSerializer.SSTable.parseFrom(data)
      val recArr = new Array[Record](internalSSTable.getRecordsCount)
      for (ind <- 0 until internalSSTable.getRecordsCount) {
        recArr(ind) = internalSSTable.getRecords(ind)
      }
      Some(SSTable(recArr))
    } catch {
      case ex: InvalidProtocolBufferException => None
    }
  }

}