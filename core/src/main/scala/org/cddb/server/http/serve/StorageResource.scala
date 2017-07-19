package org.cddb.server.http.serve

import javax.ws.rs._
import javax.ws.rs.container.{AsyncResponse, Suspended}
import javax.ws.rs.core.MediaType

import akka.actor.ActorRef
import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.cddb.store.{InsertRequest, RetrieveRequest}

case class InsReq(key: String, value: String)

@Path("/api/storage")
@Produces(Array(MediaType.APPLICATION_JSON))
class StorageResource(receiver: ActorRef) {

  import io.circe._
  import io.circe.generic.auto._

  @GET
  @Path("/{key}")
  def tryFind(@PathParam("key") key: String, @Suspended response: AsyncResponse): Unit = {
    receiver ! RetrieveRequest(key, response)
  }

  @POST
  def tryInsert(request: String, @Suspended response: AsyncResponse): Unit = {
    val decodeClipsParam = Decoder[InsReq]
    val insReq = io.circe.parser.decode(request)(decodeClipsParam).right.get
    receiver ! InsertRequest(insReq.key, insReq.value, response)
  }
}


object StorageResource {
  implicit val fooDecoder: Decoder[InsReq] = deriveDecoder[InsReq]
  implicit val fooEncoder: Encoder[InsReq] = deriveEncoder[InsReq]
}