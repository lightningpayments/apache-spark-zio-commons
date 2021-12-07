package de.commons.lib.spark

import cats.effect.{ExitCode, IO, IOApp}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Random

object RTFuture extends IOApp {
  // scalastyle:off

  private val referentialTransparencyLabel = "referential transparency"
  private val noReferentialTransparency    = s" >:( $referentialTransparencyLabel "
  private val withReferentialTransparency  = s" :D $referentialTransparencyLabel "

  private def subtractSalesTax(money: Double): Double ={
    val result = money * 100.0 / 119.0
    println(s"Your Money is $result")
    result
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val a0 =
      Future(Thread.sleep(Random.nextInt(100)))
        .flatMap(_ => Future(println("a0")))
        .flatMap(_ => Future(subtractSalesTax(100)))
    val a1 =
      Future(Thread.sleep(Random.nextInt(100)))
        .flatMap(_ => Future(println("a1")))
        .flatMap(_ => Future(subtractSalesTax(100) + subtractSalesTax(100)))
    val a2 = for {
      a <- a0
      b <- a0
      _ <- Future(println("a2"))
    } yield a + b
    println(s"***$noReferentialTransparency***")
    println("a", Await.result(a1, 5.seconds))
    println("b", Await.result(a2, 5.seconds))
    println()

    val io0 =
      IO.fromFuture(
        IO(Thread.sleep(Random.nextInt(100))) *>
        IO(println("io0")) *>
        IO(Future(subtractSalesTax(100)))
      )
    val io1 =
      IO.fromFuture(
        IO(Thread.sleep(Random.nextInt(100))) *>
        IO(println("io1")) *>
        IO(Future(subtractSalesTax(100) + subtractSalesTax(100)))
      )
    val io2 = io0.flatMap(x => io0.map(x + _))
    (for {
      _  <- IO(println("io last"))
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