package org.cddb

import scala.collection._

case class TimedObject(name: String, totalTime: Double, executions: List[Double])

case class Started(name: String, time: Long)

class Timer {
  private val timers = mutable.HashMap[String, TimedObject]()
  private val started = mutable.HashMap[String, Started]()

  def start(name: String): Unit = {
    val time = System.nanoTime()
    started.put(name, Started(name, time))
  }

  def stop(name: String): Option[TimedObject] = {
    val time = System.nanoTime()
    started.remove(name) match {
      case Some(so) => Some(record(name, time - so.time))
      case None => None
    }
  }

  def record(name: String, time: Long): TimedObject = {
    timers.get(name) match {
      case Some(to) => timers.put(name, merge(to, toTimedObject(name, time)))
      case None => timers.put(name, toTimedObject(name, time))
    }
    timers(name)
  }

  def find(name: String): Option[TimedObject] = timers.get(name)

  def toTimedObject(name: String, time: Long): TimedObject = TimedObject(name, inSec(time), List(inSec(time)))


  implicit def inSec(time: Long): Double = time.toDouble / 1000000000

  def merge(to1: TimedObject, to2: TimedObject): TimedObject =
    TimedObject(to1.name, to1.totalTime + to2.totalTime, List())


}

object Timer {

  private val timer = new Timer()

  def start(name: String) = timer.start(name)

  def stop(name: String) = timer.stop(name)

  def provide(name: String): Option[TimedObject] = timer.find(name)

}
