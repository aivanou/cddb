package org.cddb

/**
  * Created by aliaksandrivanou on 2017-07-04.
  */
object Main {

  def main(args: Array[String]): Unit = {

    println(for {
      h <- Person()
    } yield h.name)

  }

  def test(a: Seq[Seq[Int]]): Unit = {
    val m = 1
    //    a.sortBy(el => el(m))

  }

  object Person {
    def apply(): Option[Human] = Some(Human("test"))

    case class Human(name: String)

  }

}
