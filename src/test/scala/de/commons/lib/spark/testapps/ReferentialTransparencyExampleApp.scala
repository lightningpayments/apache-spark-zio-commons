package de.commons.lib.spark.testapps

import cats.implicits._
import cats.effect.{IO, IOApp}

private[testapps] object ReferentialTransparencyExampleApp extends IOApp.Simple {
  // scalastyle:off

  private def subtractSalesTax(money: Double): Double = {
    val result = money * 100 / 119
    println(s"Your Money is $result")
    result
  }

  override def run: IO[Unit] = {

    /////// RT, no
    val a0 = subtractSalesTax(100)
    val a1 = subtractSalesTax(100) + subtractSalesTax(100)
    val a2 = a0 + a0
    println(a1)
    println()
    println(a2)
    println()

    /////// RT, yes
    val io0 = IO(subtractSalesTax(100))
    val io1 = IO(subtractSalesTax(100) + subtractSalesTax(100))
    val io2 = io0 |+| io0
    (for {
      n1 <- io1
      n2 <- io2
    } yield {
      println("io1", n1)
      println()
      println("io2", n2)
      println()
    }) *> io2.map(println)

  }

  // scalastyle:on
}
