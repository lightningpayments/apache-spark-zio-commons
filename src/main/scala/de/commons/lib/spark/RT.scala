package de.commons.lib.spark

import cats.effect.{ExitCode, IO, IOApp}

object RT extends IOApp {
  // scalastyle:off

  private val referentialTransparencyLabel = "referential transparency"
  private val noReferentialTransparency    = s" >:( $referentialTransparencyLabel "
  private val withReferentialTransparency  = s" :D $referentialTransparencyLabel "

  private def subtractSalesTax(money: Double): Double = {
    val result = money * 100.0 / 119.0
    println(s"Your Money is $result")
    result
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val a0 = subtractSalesTax(100)
    val a1 = subtractSalesTax(100) + subtractSalesTax(100)
    val a2 = a0 + a0
    println(s"***$noReferentialTransparency***")
    println(a1)
    println(a2)
    println()

    val io0 = IO(subtractSalesTax(100))
    val io1 = IO(subtractSalesTax(100) + subtractSalesTax(100))
    val io2 = io0.flatMap(x => io0.map(x + _))
    (for {
      n1 <- io1
      n2 <- io2
      _  <- IO {
        println(s"***$withReferentialTransparency***")
        println(n1)
        println(n2)
        println()
      }
    } yield ()).as(ExitCode.Success)
  }

  // scalastyle:on
}