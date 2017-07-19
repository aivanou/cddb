package org.cddb.store

object StoreManager {

  def apply(): Store[StoreObject] = new InMemoryStore(new SimpleHashStrategy(), new InMemoryMetricsManager())

}
