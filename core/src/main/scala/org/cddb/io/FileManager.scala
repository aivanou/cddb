package org.cddb.io

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

class FileManager(private val path: String) {

  private val fl = init()

  def init(): RandomAccessFile = {
    new RandomAccessFile(path, "rw")
  }

  def readAll(): ByteBuffer = {
    val channel = fl.getChannel.position(0)
    readInt(channel) match {
      case Some(size) =>
        val data = ByteBuffer.allocate(size)
        val bytesRead = channel.read(data)
        if (bytesRead != size) {
          ByteBuffer.allocate(0)
        } else {
          data
        }
      case None => ByteBuffer.allocate(0)
    }
  }

  def readInt(channel: FileChannel): Option[Int] = {
    val bb = ByteBuffer.allocate(4)
    val bytesRead = channel.read(bb)
    if (bytesRead != 4) {
      None
    } else {
      bb.flip()
      Some(bb.getInt)
    }
  }

  def writeAll(bytes: ByteBuffer) = {
    val channel = fl.getChannel.position(0)
    writeInt(channel, bytes.limit())
    channel.write(bytes)
    channel.force(true)
  }

  def writeInt(channel: FileChannel, value: Int): Unit = {
    val bb = toByteBuffer(value)
    channel.write(bb)
  }

  def toByteBuffer(value: Int): ByteBuffer = {
    val bb = ByteBuffer.allocate(4)
    bb.putInt(value)
    bb.flip()
    bb
  }

  def close(): Unit = {
    fl.close()
  }

  def destroy(): Unit = {
    close()
    val f = new java.io.File(path)
    if (f.exists()) {
      f.delete()
    }
  }

}

object FileManager {
  def main(args: Array[String]): Unit = {
    val fm = FileManager("")
    fm.close()
  }

  def apply(path: String): FileManager = new FileManager(path)
}
