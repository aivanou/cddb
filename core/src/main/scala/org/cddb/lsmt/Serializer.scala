package org.cddb.lsmt


trait Serializer[T] {

  def serialize(obj: T): Array[Byte]

  def deserialize(data: Array[Byte]): Option[T]

}
