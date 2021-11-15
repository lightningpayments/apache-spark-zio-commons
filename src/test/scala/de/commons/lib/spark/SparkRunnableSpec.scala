package de.commons.lib.spark

import de.commons.lib.spark.SparkRunnable.{Runnable, RunnableR, RunnableSparkRT}
import de.commons.lib.spark.environments.SparkR
import de.commons.lib.spark.errors.SparkRunnableThrowable
import zio.{Task, ZIO}

import scala.util.Random

class SparkRunnableSpec extends TestSpec with SparkMySqlTestSupport {

  "RunnableR*" must {
    "test contravariance" in withSparkSession { implicit spark => implicit logger =>
      trait RandomTrait[T] {
        def random: Task[T]
      }
      class RandomInt extends RandomTrait[Int] {
        override def random: Task[Int] = Task(Random.nextInt())
      }

      val runnable: RunnableR[RandomInt, String] = RunnableR[RandomTrait[Int], String](Task.succeed("foo"))
      whenReady(runnable.run.provide(new RandomInt))(_ mustBe Right("foo"))
    }
  }

  "RunnableSparkRT#run" must {
    "throws an error" in {
      val t = new Throwable
      val io = ZIO.environment[SparkR] *> Task.effect[String](throw t)

      val runnable = RunnableSparkRT[SparkR, String](io).run
      whenReady(runnable.provide(new SparkR(configuration, logger))) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in {
      val io = ZIO.environment[SparkR] *> Task.succeed("foo")

      val runnable = RunnableSparkRT[SparkR, String](io).run
      whenReady(runnable.provide(new SparkR(configuration, logger)))(_ mustBe Right("foo"))
    }
  }

  "RunnableR#run" must {
    "throws an error" in withSparkSession { implicit spark => _ =>
      val t = new Throwable
      val io = ZIO.environment[SparkR] *> Task.effect[String](throw t)

      val runnable = RunnableR[SparkR, String](io).run
      whenReady(runnable.provide(new SparkR(configuration, logger))) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in withSparkSession { implicit spark => _ =>
      val io = ZIO.environment[SparkR] *> Task.succeed("foo")

      val runnable = RunnableR[SparkR, String](io).run
      whenReady(runnable.provide(new SparkR(configuration, logger)))(_ mustBe Right("foo"))
    }
  }

  "Runnable#run" must {
    "throws an error" in withSparkSession { implicit spark => _ =>
      val t = new Throwable
      val io = ZIO.environment[SparkR] *> Task.effect[String](throw t)

      val runnable = RunnableR[SparkR, String](io).run
      whenReady(runnable.provide(new SparkR(configuration, logger))) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in withSparkSession { implicit spark => _ =>
      val runnable = Runnable[String](Task.succeed("foo")).run
      whenReady(runnable)(_ mustBe Right("foo"))
    }
  }

}
