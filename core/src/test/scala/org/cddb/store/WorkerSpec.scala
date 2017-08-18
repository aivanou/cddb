package org.cddb.store

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.cddb.io.Config
import org.cddb.lsmt.{LevelHandler, TableMetadata}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}


class WorkerSpec extends TestKit(ActorSystem("WorkerSpec"))
  with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  val config = Config("./.tempdata", "index.dat", TableMetadata(500))
  val lhandler = LevelHandler(config)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    lhandler.destroy()
  }

  "A Worker actor" must {

    "send back Insert response" in {
      val worker = Props(new Worker(new LsmtStore(lhandler), "w1"))
      val echo = system.actorOf(worker)
      echo ! org.cddb.store.Insert(StoreObject("key1", "value1", System.nanoTime()), self)
      expectMsg(InsertResponse("key1"))
    }

    "send back RetrieveResponse with none for unknown key" in {
      val worker = Props(new Worker(new LsmtStore(lhandler), "w1"))
      val echo = system.actorOf(worker)
      echo ! Retrieve("unknown_key1", self)
      expectMsg(RetrieveResponse("unknown_key1", None))
    }
  }

}
