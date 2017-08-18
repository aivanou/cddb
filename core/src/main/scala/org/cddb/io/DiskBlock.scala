package org.cddb.io

import java.io.File
import java.nio.ByteBuffer
import java.util.UUID

import org.cddb.lsmt.{Serializer, TableMetadata}

import scala.io.Source

case class Config(storagePath: String, dataIndex: String, tm: TableMetadata)

class DiskBlock[T](config: Config, serializer: Serializer[T], blockId: String) {

  private val filePath = config.storagePath + "/" + blockId

  private val fm = init()

  private def init(): FileManager = {
    val dir = new File(config.storagePath)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val file = new File(filePath)
    if (!file.exists()) {
      file.createNewFile()
    }
    FileManager(filePath)
  }

  def getId: String = blockId

  def load(): Option[T] = readFromDisk()

  def save(obj: T): Unit = writeToDisk(obj)

  def readFromDisk(): Option[T] = {
    val bytes = fm.readAll()
    serializer.deserialize(bytes.array())
  }

  def writeToDisk(t: T): Unit = {
    val bytes = ByteBuffer.wrap(serializer.serialize(t))
    fm.writeAtStart(bytes)
  }

  def destroy(): Unit = {
    cleanAndClose()
    fm.destroy()
  }

  def cleanAndClose(): Unit = {
    close()
  }

  def close(): Unit = {
    fm.close()
  }

}

object DiskBlock {

  def apply[T](config: Config, serializer: Serializer[T], blockId: String): DiskBlock[T] = new DiskBlock(config, serializer, blockId)

  def main(args: Array[String]): Unit = {
    val p = "/Users/aliaksandrivanou/temp"
    val fname = UUID.randomUUID()
    val s = Source.fromFile(p + "/" + fname)
    val fl = new File(p + "/" + fname)
    s.close()
  }
}
