package de.commons.lib.spark.runnable

import de.commons.lib.spark.{SparkTestSupport, TestSpec}
import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.errors.SparkRunnableThrowable
import zio.{Task, ZIO}

class SparkIOSpec extends TestSpec with SparkTestSupport {

  "SparkIO#run" must {
    "throws an error" in {
      val t = new Throwable
      val io = for {
        _ <- ZIO.environment[SparkEnvironment]
        s <- Task.effect[String](throw t)
      } yield s

      val runnable = SparkIO[SparkEnvironment, Any, String](io).run
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

      val runnable = SparkIO[SparkEnvironment, Any, String](io).run
      whenReady(runnable.provide(new SparkEnvironment(configuration, logger)))(_ mustBe Right("foo"))
    }
  }

}
