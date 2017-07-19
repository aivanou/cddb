package org.cddb.store

import akka.actor.{Actor, ActorRef}
import akka.event.Logging

case class Init()

case class Insert(obj: StoreObject, caller: ActorRef)

case class Retrieve(key: String, caller: ActorRef)

case class Delete(key: String, caller: ActorRef)

case class InsertResponse(key: String)

case class RetrieveResponse(key: String, obj: Option[StoreObject])

case class DeleteResponse(key: String, obj: Option[StoreObject])


class Worker(store: Store[StoreObject], id: String) extends Actor {
  val log = Logging(context.system, this)

  def receive = {
    case Init() =>
    case Insert(obj: StoreObject, caller: ActorRef) =>
      log.debug(s"Received Insert : $obj")
      store.insert(obj)
      caller ! InsertResponse(obj.key)
    case Retrieve(key: String, caller: ActorRef) =>
      val ans = store.retrieve(key)
      log.debug(s"Received Retrieve $key, i have: $ans")
      caller ! RetrieveResponse(key, ans)
    case Delete(key: String, caller: ActorRef) =>
      log.debug(s"Received Delete: $key")
      caller ! DeleteResponse(key, store.delete(key))
    case _ => log.info("unknown message")
  }
}

object Worker {
}
