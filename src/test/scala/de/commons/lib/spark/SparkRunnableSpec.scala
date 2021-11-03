package de.commons.lib.spark

import de.commons.lib.spark.SparkRunnable.{SparkRZIO, SparkTask, SparkZIO}
import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.errors.SparkRunnableThrowable
import zio.{Task, ZIO}

import scala.util.Random

class SparkRunnableSpec extends TestSpec with SparkTestSupport {

  "SparkRZIO#run" must {
    "throws an error" in {
      val t = new Throwable
      val io = ZIO.environment[SparkEnvironment] *> Task.effect[String](throw t)

      val runnable = SparkRZIO[SparkEnvironment, Any, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger))) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in {
      val io = ZIO.environment[SparkEnvironment] *> Task.succeed("foo")

      val runnable = SparkRZIO[SparkEnvironment, Any, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger)))(_ mustBe Right("foo"))
    }
    "return a string -- contravariant case" in withSparkSession { implicit spark => implicit logger =>
      trait RandomTrait[T] {
        def random: Task[T]
      }
      class RandomInt extends RandomTrait[Int] {
        override def random: Task[Int] = Task(Random.nextInt())
      }

      val runnable: SparkZIO[RandomInt, String] = SparkZIO[RandomTrait[Int], String](Task.succeed("foo"))
      whenReady(runnable.run.provide(new RandomInt))(_ mustBe Right("foo"))
    }
  }

  "SparkZIO#run" must {
    "throws an error" in withSparkSession { implicit spark => _ =>
      val t = new Throwable
      val io = ZIO.environment[SparkEnvironment] *> Task.effect[String](throw t)

      val runnable = SparkZIO[SparkEnvironment, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger))) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in withSparkSession { implicit spark => _ =>
      val io = ZIO.environment[SparkEnvironment] *> Task.succeed("foo")

      val runnable = SparkZIO[SparkEnvironment, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger)))(_ mustBe Right("foo"))
    }
  }

  "SparkTask#run" must {
    "throws an error" in withSparkSession { implicit spark => _ =>
      val t = new Throwable
      val io = ZIO.environment[SparkEnvironment] *> Task.effect[String](throw t)

      val runnable = SparkZIO[SparkEnvironment, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger))) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in withSparkSession { implicit spark => _ =>
      val runnable = SparkTask[String](Task.succeed("foo")).run
      whenReady(runnable)(_ mustBe Right("foo"))
    }
  }

}
