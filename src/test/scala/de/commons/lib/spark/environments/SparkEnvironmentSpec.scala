package de.commons.lib.spark.environments

import cats.implicits._
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
      val f: (SparkSession, Logger) => Int = { case (_, _) => 1 }
      whenReady(env.apply(f))(_ mustBe Right(1))
    }
    "applyR" in {
      val env = new SparkEnvironment(configuration, logger)
      val f: (SparkSession, Logger) => Int = { case (_, _) => 1 }

      val program = env.applyR(ZIO.environment[ZEnv] >>> Task(f)).provideCustomLayer(ZEnv.live)
      runtime.unsafeRun(program) mustBe 1
    }
    "liftF" in {
      val f: String => (SparkSession, Logger) => Int = {
        _ => {
          case (_, _) => 1
        }
      }
      val env = new SparkEnvironment(configuration, logger)
      val program: Task[Int] = env.liftF(Task("foo"))(f).flatten
      whenReady(program)(_ mustBe Right(1))
    }
    "liftR" in {
      val f: Int => (SparkSession, Logger) => Int = {
        _ => {
          case (_, _) => 1
        }
      }
      val env = new SparkEnvironment(configuration, logger)
      val program = env.liftR(Task(1))(f).flatten
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

  "SparkEnvironment combine test" must {
    "return a 'foobar' string" in {
      val g: String => (SparkSession, Logger) => String = {
        foo => {
          case (_, _) => foo
        }
      }
      val f: (SparkSession, Logger) => String = { case (_, _) => "bar" }
      val p = (for {
        env    <- ZIO.environment[SparkEnvironment]
        p1      = env.liftF(Option("foo"))(g)
        p2      = env.applyR(ZIO(f))
        maybe  <- p1.flatMap(ops => p2.map(bar => ops.map(_ + bar)))
        foobar  = maybe.fold(fail)(identity)
      } yield foobar).provide(new SparkEnvironment(configuration, logger))

      whenReady(p)(_ mustBe Right("foobar"))
    }
  }

}
