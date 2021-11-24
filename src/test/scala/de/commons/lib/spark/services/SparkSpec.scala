package de.commons.lib.spark.services

import de.commons.lib.spark.{SparkMySqlTestSupport, TestSpec}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import zio.internal.Platform
import zio.interop.catz.monadErrorInstance
import zio.{Task, ZEnv, ZIO}

import scala.util.Try

class SparkSpec extends TestSpec with SparkMySqlTestSupport {

  case class Dummy(value: Int)
  object Dummy {
    implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]
  }

  "Spark" must {
    val env = Spark.apply(configuration, logger)

    "sparkM" in {
      whenReady(env.provideLayer(Spark.live).>>=(_.sparkM)) {
        case Left(_) => fail()
        case Right(value) => value mustBe a[SparkSession]
      }
    }
    "apply" in {
      case class Service(spark: SparkSession, logger: Logger) {
        def createDummies(dummies: Dummy*): Dataset[Dummy] = {
          import Dummy._
          logger.info("create dataset of dummies")
          spark.createDataset(dummies)
        }
      }

      val live = env.provideLayer(Spark.live)
      val program =
        live.>>=(_.apply(Service.apply).map(_.createDummies(Dummy(1), Dummy(2), Dummy(3))).map(_.collect().toList))

      whenReady(program) {
        case Right(unordered) => unordered.sortBy(_.value) mustBe (Dummy(1) :: Dummy(2) :: Dummy(3) :: Nil)
        case Left(_) => fail()
      }
    }
    "applyR" in {
      def f(spark: SparkSession, logger: Logger): Int = 1
      val program = env
        .provideLayer(Spark.live)
        .>>=(_.applyR(ZIO.environment[ZEnv] >>> Task[(SparkSession, Logger) => Int](f)).provideLayer(ZEnv.live))

      runtime.unsafeRun(program) mustBe 1
    }
    "lift" in {
      def f(spark: SparkSession, logger: Logger)(a: String): Int = 1

      val live = env.provideLayer(Spark.live)

      val program1: Task[Int] = live.>>=(_.lift(f)(Task("foo")).flatten)
      whenReady(program1)(_ mustBe Right(1))

      case class Dummy(spark: SparkSession, logger: Logger) {
        def identity(a: String): String = a
      }
      val program2: Task[String] =
        live.>>=(_.lift((spark, logger) => Dummy(spark, logger).identity)(Task("foo")).flatten)
      whenReady(program2)(_ mustBe Right("foo"))
    }
    "liftR" in {
      def f(spark: SparkSession, logger: Logger)(a: Int): Int = 1

      val live = env.provideLayer(Spark.live)

      whenReady(live.flatMap(_.liftR(f)(Task(1)).flatten))(_ mustBe Right(1))

      case class Dummy(spark: SparkSession, logger: Logger) {
        def identity(a: String): String = a
      }
      val program2: Task[String] =
        live.flatMap(_.liftR((spark, logger) => Dummy(spark, logger).identity)(Task("foo"))).flatten
      whenReady(program2)(_ mustBe Right("foo"))
    }
    "raiseError" in {
      whenReady(env.provideLayer(Spark.live).flatMap(_.raiseError[String](new Throwable)))(_.isLeft mustBe true)
    }
    "raiseErrorR" in {
      val runtime = zio.Runtime(env, Platform.default)

      Try(runtime.unsafeRun(env.provideLayer(Spark.live).flatMap(_.raiseErrorR(new Throwable)))).toOption mustBe None
    }
  }

}
