package org.cddb.store

import javax.ws.rs.container.AsyncResponse

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import io.circe._
import io.circe.generic.semiauto._

import scala.collection._

case class InsertRequest(key: String, value: String, response: AsyncResponse)

case class RetrieveRequest(key: String, response: AsyncResponse)

case class DeleteRequest(key: String, response: AsyncResponse)

class Receiver(master: ActorRef) extends Actor {
  val log = Logging(context.system, this)

  private val waitingForResponse = new mutable.HashMap[String, AsyncResponse]()

  implicit val storeDecoder: Decoder[StoreObject] = deriveDecoder[StoreObject]
  implicit val storeEncoder: Encoder[StoreObject] = deriveEncoder[StoreObject]

  def receive = {
    case Init() =>
    case RetrieveRequest(key: String, response: AsyncResponse) =>
      log.debug(s"Received Retrieve request for key: $key")
      waitingForResponse.put(key, response)
      master ! Retrieve(key, self)
    case DeleteRequest(key: String, response: AsyncResponse) =>
      log.debug(s"Received Delete request for key: $key")
      waitingForResponse.put(key, response)
      master ! Delete(key, self)
    case InsertRequest(key, value, response) =>
      log.debug(s"Received Insert request for {$key : $value}")
      waitingForResponse.put(key, response)
      master ! Insert(StoreObject(key, value, System.nanoTime()), self)
    case InsertResponse(key: String) => waitingForResponse.remove(key) match {
      case Some(response) => response.resume(s"Inserted key: $key")
      case None =>
        log.warning(s"Received response from worker for key: $key that I don't have")
    }
    case RetrieveResponse(key: String, obj: Option[StoreObject]) => waitingForResponse.remove(key) match {
      case Some(response) => obj match {
        case Some(out) => response.resume(storeEncoder(out).toString())
        case None => response.resume(s"Not found: $key")
      }
      case None =>
        log.warning(s"Received response from worker for key: $key that I don't have")
    }
    case DeleteResponse(key: String, obj: Option[StoreObject]) => waitingForResponse.remove(key) match {
      case Some(response) => obj match {
        case Some(out) => response.resume(storeEncoder(out).toString())
        case None => response.resume(s"Not found: $key")
      }
      case None =>
        log.warning(s"Received response from worker for key: $key that I don't have")
    }
    case _ => log.info("unknown message")
  }
}

object Receiver {

}
