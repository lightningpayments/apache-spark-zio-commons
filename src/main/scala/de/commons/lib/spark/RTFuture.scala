package de.commons.lib.spark

import cats.effect.{ExitCode, IO, IOApp}

import java.time.LocalDateTime
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
      Future(println("a0" + LocalDateTime.now()))
        .flatMap(_ => Future(Thread.sleep(Random.nextInt(100))))
        .flatMap(_ => Future(subtractSalesTax(100)))
    val a1 =
      Future(println("a1" + LocalDateTime.now()))
        .flatMap(_ => Future(Thread.sleep(Random.nextInt(100))))
        .flatMap(_ => Future(subtractSalesTax(100) + subtractSalesTax(100)))
    val a2 = for {
      _ <- Future(println("a2" + LocalDateTime.now()))
      a <- a0
      b <- a0
    } yield a + b
    println(s"***$noReferentialTransparency***")
    println("a1", Await.result(a1, 5.seconds))
    println("a2", Await.result(a2, 5.seconds))
    println()

    val io0 =
      IO.fromFuture(
        IO(println("io0" + LocalDateTime.now())) *>
        IO(Thread.sleep(Random.nextInt(100))) *>
        IO(Future(subtractSalesTax(100)))
      )
    val io1 =
      IO.fromFuture(
        IO(println("io1" + LocalDateTime.now())) *>
        IO(Thread.sleep(Random.nextInt(100))) *>
        IO(Future(subtractSalesTax(100) + subtractSalesTax(100)))
      )
    val io2 = IO(println("io2" + LocalDateTime.now())) *> io0.flatMap(x => io0.map(x + _))
    (for {
      _  <- IO(println("io2" + LocalDateTime.now()))
      o0 <- io1
      o1 <- io2
      _  <- IO {
        println(s"***$withReferentialTransparency***")
        println("io1", o0)
        println("io2", o1)
        println()
      }
    } yield ()).as(ExitCode.Success)
  }

  // scalastyle:on
}