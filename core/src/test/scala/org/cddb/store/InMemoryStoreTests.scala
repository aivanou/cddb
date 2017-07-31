package org.cddb.store

import org.scalatest.{FlatSpec, Matchers}

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class InMemoryStoreTests extends FlatSpec with Matchers {

  "A simple store operations" should "run successfully" in {
    val hashStrategy = new SimpleHashStrategy()
    val metrics = new InMemoryMetricsManager()
    val store = new InMemoryStore(hashStrategy, metrics)
    val obj1 = StoreObject("test key1", "test value1", System.nanoTime())
    val obj2 = StoreObject("test key2", "test value2", System.nanoTime())
    val obj3 = StoreObject("test key3", "test value3", System.nanoTime())
    store.insert(obj1)
    val retrievedObj = store.retrieve("test key1")
    retrievedObj shouldEqual Some(obj1)
  }

}
