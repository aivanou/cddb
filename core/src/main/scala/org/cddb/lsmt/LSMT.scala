package org.cddb.lsmt

import scala.collection._

case class TableMetadata(maxSize: Int, softThreshold: Double, persistSize: Int)

case class Result()

case class Record(key: String, value: String, status: String, timestamp: Long)

case class IndexRecord(key: String, offset: Long)

case class Index(records: List[IndexRecord])


object LSMT {
  def main(args: Array[String]): Unit = {
    var data = immutable.TreeMap[Int, Int]()
    data = data + ((1, 2))
    println(data)
    data = data + ((1, 3))
    println(data)
  }
}
