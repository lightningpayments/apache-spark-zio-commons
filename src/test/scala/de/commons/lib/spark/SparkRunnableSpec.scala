package de.commons.lib.spark

import de.commons.lib.spark.environments.Spark
import de.commons.lib.spark.errors.SparkRunnableThrowable
import zio.NeedsEnv.needsEnvAmbiguous1
import zio.{Has, Task, ZIO, ZLayer}

import scala.util.Random

class SparkRunnableSpec extends TestSpec with SparkMySqlTestSupport {

  "SparkRunnableR*" must {
    "test contravariance" in withSparkSession { implicit spark => implicit logger =>
      trait RandomTrait[T] {
        def random: Task[T]
      }
      class RandomInt extends RandomTrait[Int] {
        override def random: Task[Int] = Task(Random.nextInt())
      }

      val p1 = SparkRunnable[String](Task("foo"))
      val p2 = ZIO.accessM[Has[RandomInt]](_.get.random)
      val layer = ZLayer.succeed(new RandomInt) ++ Spark.live

      whenReady((p2 *> p1.run).provideLayer(layer))(_ mustBe Right("foo"))
    }
  }

  "SparkRunnableR#run" must {
    "throws an error" in withSparkSession { implicit spark => implicit logger =>
      val t = new Throwable
      val io = Task.effect[String](throw t)

      val runnable = SparkRunnable[String](io)

      whenReady(runnable.provideLayer(Spark.live)) {
        case Right(_) => fail()
        case Left(ex) =>
          ex mustBe SparkRunnableThrowable(t)
          ex.getMessage mustBe SparkRunnableThrowable(t).getMessage
      }
    }
    "return a string when successful" in withSparkSession { implicit spark => implicit logger =>
      val runnable = SparkRunnable[String](Task("foo"))
      whenReady(runnable.run.provideLayer(Spark.live))(_ mustBe Right("foo"))
    }
  }

}
