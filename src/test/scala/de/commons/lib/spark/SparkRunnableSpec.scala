package de.commons.lib.spark

import de.commons.lib.spark.SparkRunnable.{SparkRZIO, SparkTask, SparkZIO}
import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.errors.SparkRunnableThrowable
import zio.{Task, ZIO}

class SparkRunnableSpec extends TestSpec with SparkTestSupport {

  "SparkRZIO#run" must {
    "throws an error" in {
      val t = new Throwable
      val io = for {
        _ <- ZIO.environment[SparkEnvironment]
        s <- Task.effect[String](throw t)
      } yield s

      val runnable = SparkRZIO[SparkEnvironment, Any, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger))) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in {
      val io = for {
        _ <- ZIO.environment[SparkEnvironment]
        s <- Task.succeed("foo")
      } yield s

      val runnable = SparkRZIO[SparkEnvironment, Any, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger)))(_ mustBe Right("foo"))
    }
  }

  "SparkZIO#run" must {
    "throws an error" in withSparkSession { implicit spark => _ =>
      val t = new Throwable
      val io = for {
        _ <- ZIO.environment[SparkEnvironment]
        s <- Task.effect[String](throw t)
      } yield s

      val runnable = SparkZIO[SparkEnvironment, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger))) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in withSparkSession { implicit spark => _ =>
      val io = for {
        _ <- ZIO.environment[SparkEnvironment]
        s <- Task.succeed("foo")
      } yield s

      val runnable = SparkZIO[SparkEnvironment, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger)))(_ mustBe Right("foo"))
    }
  }

  "SparkTask#run" must {
    "throws an error" in withSparkSession { implicit spark => _ =>
      val t = new Throwable
      val io = for {
        _ <- ZIO.environment[SparkEnvironment]
        s <- Task.effect[String](throw t)
      } yield s

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
