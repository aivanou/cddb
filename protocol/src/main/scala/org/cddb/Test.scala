package org.cddb

import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import org.cddb.protocol.internal.GreeterGrpc
import org.cddb.protocol.internal.Protocol.{HelloReply, HelloRequest}

object Main {
  def main(args: Array[String]): Unit = {
    val server = ServerBuilder.forPort(44001)
      //      .addService(new GreeterImpl())
      .build()
      .start();

  }

  class GreeterRpc extends GreeterGrpc.GreeterImplBase {
    override def sayHello(request: HelloRequest, responseObserver: StreamObserver[HelloReply]): Unit = {
      super.sayHello(request, responseObserver)

    }
  }

}