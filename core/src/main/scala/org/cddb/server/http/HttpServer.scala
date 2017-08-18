package org.cddb.server.http

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.dropwizard.setup.Environment
import io.dropwizard.{Application, Configuration}
import org.cddb.io.Config
import org.cddb.lsmt.{LevelHandler, TableMetadata}
import org.cddb.server.http.serve.StorageResource
import org.cddb.store._

class HttpServer extends Application[HttpServerConf] {
  override def run(conf: HttpServerConf, env: Environment): Unit = {
    val conf = ConfigFactory.parseFile(new File("conf/akka.conf"))
    val system = ActorSystem("test", conf)
    val lsmtConfig = Config("/Users/aliaksandrivanou/temp/data", "index.dat", TableMetadata(500))
    val internalHandler = LevelHandler(lsmtConfig)
    val worker = (name: String) => system.actorOf(Props(new Worker(new LsmtStore(internalHandler), name)), name)
    val distributor = new Distributor(worker)
    val master = system.actorOf(Props(new Master(distributor, Master.hashFunc)))
    val receiver = system.actorOf(Props(new Receiver(master)))
    env.jersey().register(new StorageResource(receiver))
  }
}

object HttpServer {
  def main(args: Array[String]): Unit = {
    new HttpServer().run("server")
  }
}

class HttpServerConf extends Configuration {}
