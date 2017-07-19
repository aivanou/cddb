package org.cddb.store

case class Metrics(numStoredKeys: Int, numRetrievedKeys: Int, numrRemovedKeys: Int)

trait MetricsManager {

  def registerStore()

  def registerRetrieve()

  def registerRemove()

  def getMetrics(): Metrics

}

class InMemoryMetricsManager extends MetricsManager {

  private var numStoredKeys: Int = 0
  private var numRetrievedKeys: Int = 0
  private var numRemovedKeys: Int = 0

  override def registerStore(): Unit = numStoredKeys += 1

  override def registerRetrieve(): Unit = numRetrievedKeys += 1

  override def registerRemove(): Unit = numRemovedKeys += 1

  override def getMetrics(): Metrics = Metrics(numStoredKeys, numRetrievedKeys, numRemovedKeys)
}

object Metrics {
  def apply(): MetricsManager = new InMemoryMetricsManager()
}