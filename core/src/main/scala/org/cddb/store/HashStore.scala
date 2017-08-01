package org.cddb.store

import org.cddb.lsmt.{LevelHandler, Record}

import scala.collection.mutable

case class StoreObject(key: String, value: String, timestamp: Long)

trait HashStrategy {
  def computeHash(el: String): String
}

class SimpleHashStrategy extends HashStrategy {
  override def computeHash(el: String): String = el
}

trait Store[T] {

  def insert(obj: T): Unit

  def retrieve(key: String): Option[T]

  def delete(key: String): Option[T]

}

class LsmtStore(levelHandler: LevelHandler) extends Store[StoreObject] {

  implicit def toInternalRecord(obj: StoreObject): Record =
    Record(obj.key, obj.value, "", obj.timestamp)

  implicit def toExternalObj(rec: Record): StoreObject =
    StoreObject(rec.key, rec.value, rec.timestamp)


  override def insert(obj: StoreObject): Unit = levelHandler.append(obj)

  override def retrieve(key: String): Option[StoreObject] = levelHandler.read(key) match {
    case Some(obj) => Some(obj)
    case None => None
  }

  override def delete(key: String): Option[StoreObject] = {
    levelHandler.append(StoreObject(key, key, System.nanoTime()))
    retrieve(key)
  }
}

class InMemoryStore(hashStrategy: HashStrategy, metrics: MetricsManager) extends Store[StoreObject] {

  private[store] val table = new mutable.HashMap[String, StoreObject]()

  override def insert(obj: StoreObject): Unit = {
    val hash = hashStrategy.computeHash(obj.key)
    table.put(hash, obj)
    metrics.registerStore()
  }

  override def retrieve(key: String): Option[StoreObject] = {
    metrics.registerRetrieve()
    table.get(hashStrategy.computeHash(key))
  }

  override def delete(key: String): Option[StoreObject] = {
    val hash = hashStrategy.computeHash(key.toString)
    if (table.contains(hash)) {
      metrics.registerRemove()
    }
    table.remove(hash)
  }
}

object Store {

  def apply(strategy: HashStrategy, metricsManager: MetricsManager): Store[StoreObject] =
    new InMemoryStore(strategy, metricsManager)
}
