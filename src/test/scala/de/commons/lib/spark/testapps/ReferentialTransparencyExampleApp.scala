package de.commons.lib.spark.testapps

import zio.{ExitCode, URIO, ZIO}

private[testapps] object ReferentialTransparencyExampleApp extends zio.App {

  // scalastyle:off

  private def subtractSalesTax(money: Double): Double = {
    val result = money * 100 / 119
    println(s"Your Money is $result")
    result
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    /////// RT, no
    val a0 = subtractSalesTax(100)
    val a1 = subtractSalesTax(100) + subtractSalesTax(100)
    val a2 = a0 + a0
    println("RT, no", a1)
    println()
    println("RT, no", a2)
    println()

    /////// RT, yes
    val io0 = ZIO(subtractSalesTax(100))
    val io1 = ZIO(subtractSalesTax(100) + subtractSalesTax(100))
    val io2 = io0 <<< io0

    ((for {
      n1 <- io1
      n2 <- io2
    } yield {
      println("RT yes", n1)
      println()
      println("RT yes", n2)
      println()
    }) *> io2.map(println("RT yes", _))).exitCode

  }

  // scalastyle:on

}
