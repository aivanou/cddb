package org.cddb.store

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.cddb.io.Config
import org.cddb.lsmt.{LevelHandler, TableMetadata}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}


class MasterSpec extends TestKit(ActorSystem("WorkerSpec"))
  with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  val config = Config("./.tempdata", "index.dat", TableMetadata(500))
  val lhandler = LevelHandler(config)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    lhandler.destroy()
  }

  "A Master actor" must {

    "send Insert to worker" in {
      val w1 = TestProbe()
      val distributor = new Distributor(name => w1.ref)
      val master = system.actorOf(Props(new Master(distributor, str => str.hashCode.toLong)))
      var sobj = StoreObject("key1", "value1", System.nanoTime())
      master ! Insert(sobj, w1.ref)
      w1.expectMsg(Insert(sobj, w1.ref))
      sobj = StoreObject("unknown_key1", "value1", System.nanoTime())
      master ! Retrieve(sobj.key, w1.ref)
      w1.expectMsg(Retrieve(sobj.key, w1.ref))
    }
  }

}
