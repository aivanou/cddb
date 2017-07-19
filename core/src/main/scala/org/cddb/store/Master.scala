package org.cddb.store

import akka.actor.{Actor, ActorRef}
import akka.event.Logging

import scala.util.hashing.MurmurHash3

case class Range(start: Long, end: Long)

class Master(distributor: Distributor, hashFunc: (String => Long)) extends Actor {
  val log = Logging(context.system, this)

  override def receive = {
    case Init() =>
    case Insert(obj: StoreObject, caller: ActorRef) =>
      log.debug(s"Received Insert message: $obj")
      distributor.distribute(hashFunc(obj.key)) ! Insert(obj, caller)
    case Retrieve(key: String, caller: ActorRef) =>
      log.debug(s"Received Retrieve message: $key, caller: $caller")
      distributor.distribute(hashFunc(key)) ! Retrieve(key, caller)
    case Delete(key: String, caller: ActorRef) =>
      log.debug(s"Received Delete message: $key, caller: $caller")
      distributor.distribute(hashFunc(key)) ! Delete(key, caller)
    case _ => log.info("unknown message")
  }

}

class Distributor(f: (String => ActorRef)) {

  private val ranges = Array.fill[Range](1)(Range(0, Long.MaxValue))
  private val nodes = Array.fill[ActorRef](1)(f("test"))

  def distribute(hash: Long): ActorRef = nodes(0)

}

object Master {

  def hashFunc(str: String): Long = {
    val a = str.toCharArray.map(_.toByte)
    val hash1 = MurmurHash3.arrayHash(a)
    val hash2 = MurmurHash3.arrayHash(a.reverse)
    hash1 * hash2
  }

  def main(args: Array[String]): Unit = {
  }

}
