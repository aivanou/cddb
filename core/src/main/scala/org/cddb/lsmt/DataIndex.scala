package org.cddb.lsmt

import com.google.protobuf.InvalidProtocolBufferException
import org.cddb.lsmt.internal.TableSerializer

case class Metadata(range: Range, blockId: String)

case class DataIndex(indices: List[Metadata])

object DataIndex extends Serializer[DataIndex] {

  implicit def toProtoMetadata(rec: Metadata): org.cddb.lsmt.internal.TableSerializer.Metadata =
    TableSerializer.Metadata.newBuilder()
      .setFrom(rec.range.from)
      .setTo(rec.range.to)
      .setBlockId(rec.blockId).build()

  implicit def toMetadata(mt: TableSerializer.Metadata): Metadata =
    Metadata(Range(mt.getFrom, mt.getTo, mt.getTimestamp), mt.getBlockId)

  implicit def toProtoDataIndex(d: DataIndex): org.cddb.lsmt.internal.TableSerializer.DataIndex = {
    val bldr = TableSerializer.DataIndex.newBuilder()
    d.indices.foreach(ind => bldr.addIndices(ind))
    bldr.build()
  }

  implicit def toDataIndex(dt: TableSerializer.DataIndex): DataIndex = {
    val arr = new Array[Metadata](dt.getIndicesCount)
    for (i <- 0 until dt.getIndicesCount) {
      arr(i) = dt.getIndices(i)
    }
    new DataIndex(arr.toList)
  }


  override def serialize(ind: DataIndex): Array[Byte] = ind.toByteArray

  override def deserialize(data: Array[Byte]): Option[DataIndex] = {
    try {
      val result = TableSerializer.DataIndex.parseFrom(data)
      Some(result)
    } catch {
      case ex: InvalidProtocolBufferException => None
    }
  }
}