package org.cddb.io

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

class FileManager(private val path: String) {

  private val fl = init()

  def init(): RandomAccessFile = {
    new RandomAccessFile(path, "rw")
  }

  def setPosition(pos: Int): Unit = {
    fl.seek(pos)
  }

  def reset(): Unit = setPosition(0)

  def readAll(): ByteBuffer = {
    val channel = fl.getChannel.position(0)
    readInt(channel) match {
      case Some(size) =>
        val data = ByteBuffer.allocate(size)
        val bytesRead = channel.read(data)
        if (bytesRead != size) {
          ByteBuffer.allocate(0)
        } else {
          data.flip()
          data
        }
      case None => ByteBuffer.allocate(0)
    }
  }

  def read(offset: Int, size: Int): (ByteBuffer, Int) = {
    val channel = fl.getChannel.position(offset)
    read(channel, size)
  }

  def read(size: Int): (ByteBuffer, Int) = {
    val channel = fl.getChannel
    read(channel, size)
  }

  def read(channel: FileChannel, size: Int): (ByteBuffer, Int) = {
    val data = ByteBuffer.allocate(size)
    val bytesRead = channel.read(data)
    data.flip()
    (data, bytesRead)
  }

  def getPosition(): Long = fl.getChannel.position()

  def readInt(): Option[Int] = readInt(fl.getChannel)

  def readInt(pos: Int): Option[Int] = readInt(fl.getChannel.position(pos))

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

  def write(bytes: ByteBuffer): Unit = {
    val channel = fl.getChannel
    write(channel, bytes)
  }

  def write(pos: Int, bytes: ByteBuffer): Unit = {
    val channel = fl.getChannel.position(pos)
    write(channel, bytes)
  }

  private def write(channel: FileChannel, bytes: ByteBuffer): Unit = {
    channel.write(bytes)
  }

  def writeAtStart(bytes: ByteBuffer) = {
    val channel = fl.getChannel.position(0)
    write(channel, bytes)
  }

  def flush(): Unit = fl.getChannel.force(true)

  def writeInt(value: Int): Unit = writeInt(fl.getChannel, value)

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

  def create(path: String, filename: String): Unit = {
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val file = new File(path + "/" + file)
    if (!file.exists()) {
      file.createNewFile()
    }
  }
}
