package de.commons.lib.spark.environments

import de.commons.lib.spark.environments.SparkR._
import de.commons.lib.spark.{SparkTestSupport, TestSpec}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import zio.internal.Platform
import zio.interop.catz.monadErrorInstance
import zio.{Task, ZEnv, ZIO}

import scala.util.Try

class SparkEnvironmentSpec extends TestSpec with SparkTestSupport {

  "SparkEnvironment" must {
    "sparkM" in {
      val env = new SparkEnvironment(configuration, logger)
      whenReady(env.sparkM) {
        case Left(_) => fail()
        case Right(value) => value mustBe a[SparkSession]
      }
    }
    "apply" in {
      val env = new SparkEnvironment(configuration, logger)
      def f(spark: SparkSession, logger: Logger): Int = 1

      whenReady(env.apply(f))(_ mustBe Right(1))
    }
    "applyR" in {
      val env = new SparkEnvironment(configuration, logger)
      def f(spark: SparkSession, logger: Logger): Int = 1
      val program = env
        .applyR(ZIO.environment[ZEnv] >>> Task[(SparkSession, Logger) => Int](f))
        .provideCustomLayer(ZEnv.live)

      runtime.unsafeRun(program) mustBe 1
    }
    "lift" in {
      def f(a: String)(spark: SparkSession, logger: Logger): Int = 1
      val env = new SparkEnvironment(configuration, logger)
      val program: Task[Int] = env.lift(f)(Task("foo")).flatten

      whenReady(program)(_ mustBe Right(1))
    }
    "liftR" in {
      def f(a: Int)(spark: SparkSession, logger: Logger): Int = 1
      val env = new SparkEnvironment(configuration, logger)
      val program = env.liftR(f)(Task(1)).flatten

      whenReady(program)(_ mustBe Right(1))
    }
    "raiseError" in {
      val env = new SparkEnvironment(configuration, logger)

      whenReady(env.raiseError[String](new Throwable))(_.isLeft mustBe true)
    }
    "raiseErrorR" in {
      val env = new SparkEnvironment(configuration, logger)
      val runtime = zio.Runtime(env, Platform.default)

      Try(runtime.unsafeRun(env.raiseErrorR(new Throwable))).toOption mustBe None
    }
  }

}
