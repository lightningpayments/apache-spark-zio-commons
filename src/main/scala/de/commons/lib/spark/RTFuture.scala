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
      Future {
        Thread.sleep(Random.nextInt(1000))
        println("a0 " + LocalDateTime.now())
        subtractSalesTax(100)
      }
    val a1 =
      Future {
        Thread.sleep(Random.nextInt(1000))
        println("a1 " + LocalDateTime.now())
        subtractSalesTax(100) + subtractSalesTax(100)
      }
    val a2 = for {
      _ <- Future(println("a2 " + LocalDateTime.now()))
      a <- a0
      b <- a0
    } yield a + b
    println(s"***$noReferentialTransparency***")
    println("a1 result", Await.result(a1, 5.seconds))
    println("a2 result", Await.result(a2, 5.seconds))
    println()

    val io0 =
      IO.fromFuture(
        IO(Thread.sleep(Random.nextInt(1000))) *>
        IO(println("io0 " + LocalDateTime.now())) *>
        IO(Future(subtractSalesTax(100)))
      )
    val io1 =
      IO.fromFuture(IO(Future {
        Thread.sleep(Random.nextInt(1000))
        println("io1 " + LocalDateTime.now())
        subtractSalesTax(100) + subtractSalesTax(100)
      }))
    val io2 = IO(println("io2 " + LocalDateTime.now())) *> io0.flatMap(x => io0.map(x + _))
    (for {
      _  <- IO(println(s"***$withReferentialTransparency***"))
      o0 <- io1
      o1 <- io2
      _  <- IO {
        println("io1 result ", o0)
        println("io2 result ", o1)
        println()
      }
    } yield ()).as(ExitCode.Success)
  }

  // scalastyle:on
}