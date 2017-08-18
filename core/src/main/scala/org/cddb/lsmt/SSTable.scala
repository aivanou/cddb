package org.cddb.lsmt

/**
  * Represents the common functionality for immutable string table.
  */
abstract class SSTable[T] extends Table {

  def getSize: Int

  def getRange: Range

  def iterator: Iterator[T]

  def tryRead(key: String): Option[T]

  def destroy(): Unit

  def cleanAndClose(): Unit

}