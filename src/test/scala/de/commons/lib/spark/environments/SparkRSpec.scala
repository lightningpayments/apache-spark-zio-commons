package de.commons.lib.spark.environments

import de.commons.lib.spark.{SparkMySqlTestSupport, TestSpec}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import zio.internal.Platform
import zio.interop.catz.monadErrorInstance
import zio.{Task, ZEnv, ZIO}

import scala.util.Try

class SparkRSpec extends TestSpec with SparkMySqlTestSupport {

  case class Dummy(value: Int)
  object Dummy {
    implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]
  }

  "SparkR" must {
    "sparkM" in {
      val env = new SparkR(configuration, logger)
      whenReady(env.sparkM) {
        case Left(_) => fail()
        case Right(value) => value mustBe a[SparkSession]
      }
    }
    "apply" in {
      val env = new SparkR(configuration, logger)

      case class Service(spark: SparkSession, logger: Logger) {
        def createDummies(dummies: Dummy*): Dataset[Dummy] = {
          import Dummy._
          logger.info("create dataset of dummies")
          spark.createDataset(dummies)
        }
      }

      whenReady(env.apply(Service.apply).map(_.createDummies(Dummy(1), Dummy(2), Dummy(3))).map(_.collect().toList)) {
        case Right(unordered) => unordered.sortBy(_.value) mustBe (Dummy(1) :: Dummy(2) :: Dummy(3) :: Nil)
        case Left(_) => fail()
      }
    }
    "applyR" in {
      val env = new SparkR(configuration, logger)
      def f(spark: SparkSession, logger: Logger): Int = 1
      val program = env
        .applyR(ZIO.environment[ZEnv] >>> Task[(SparkSession, Logger) => Int](f))
        .provideCustomLayer(ZEnv.live)

      runtime.unsafeRun(program) mustBe 1
    }
    "lift" in {
      def f(spark: SparkSession, logger: Logger)(a: String): Int = 1
      val env = new SparkR(configuration, logger)
      val program1: Task[Int] = env.lift(f)(Task("foo")).flatten
      whenReady(program1)(_ mustBe Right(1))

      case class Dummy(spark: SparkSession, logger: Logger) {
        def identity(a: String): String = a
      }
      val program2: Task[String] = env.lift((spark, logger) => Dummy(spark, logger).identity)(Task("foo")).flatten
      whenReady(program2)(_ mustBe Right("foo"))
    }
    "liftR" in {
      def f(spark: SparkSession, logger: Logger)(a: Int): Int = 1
      val env = new SparkR(configuration, logger)
      val program1 = env.liftR(f)(Task(1)).flatten
      whenReady(program1)(_ mustBe Right(1))

      case class Dummy(spark: SparkSession, logger: Logger) {
        def identity(a: String): String = a
      }
      val program2: Task[String] = env.liftR((spark, logger) => Dummy(spark, logger).identity)(Task("foo")).flatten
      whenReady(program2)(_ mustBe Right("foo"))
    }
    "raiseError" in {
      val env = new SparkR(configuration, logger)

      whenReady(env.raiseError[String](new Throwable))(_.isLeft mustBe true)
    }
    "raiseErrorR" in {
      val env = new SparkR(configuration, logger)
      val runtime = zio.Runtime(env, Platform.default)

      Try(runtime.unsafeRun(env.raiseErrorR(new Throwable))).toOption mustBe None
    }
  }

}
