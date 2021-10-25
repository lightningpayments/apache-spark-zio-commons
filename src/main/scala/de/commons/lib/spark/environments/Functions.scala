package de.commons.lib.spark.environments

import cats.Monad
import de.commons.lib.spark.SparkSessionLoader
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import play.api.Configuration
import zio.{Task, ZIO}

import scala.language.higherKinds
import scala.util.Try

private[environments] trait Functions {

  val configuration: Configuration
  val logger: Logger

  val sparkM: Task[SparkSession] =
    for {
      loader <- Task.succeed(new SparkSessionLoader(configuration))
      spark  <- Task.fromTry(Try(loader.getSpark))
    } yield spark

  val loggerM: Task[Logger] = Task.succeed(logger)

  val unit: Task[Unit] = Task.unit

  def apply[A](f: (SparkSession, Logger) => A): Task[A] =
    ZIO.tupledPar(sparkM, loggerM).map {
      case (session, logger) => f(session, logger)
    }

  def applyR[R, A](ff: ZIO[R, Throwable, (SparkSession, Logger) => A]): ZIO[R, Throwable, A] =
    ZIO.tupledPar(sparkM, loggerM).flatMap {
      case (session, logger) => ff.map(_(session, logger))
    }

  def liftF[F[_], A, B](fa: => F[A])(f: A => (SparkSession, Logger) => B)(implicit M: Monad[F]): Task[F[B]] =
    ZIO.tupledPar(sparkM, loggerM).flatMap {
      case (session, logger) => Task(M.map(fa)(f(_)(session, logger)))
    }

  def liftR[R, F[_], A, B](
    fa: => F[A])(
    f: A => (SparkSession, Logger) => B)(
    implicit M: Monad[F]
  ): ZIO[R, Throwable, F[B]] =
    ZIO.environment[R] >>> liftF(fa)(f)

  def raiseError[A](e: Throwable): Task[A] = Task.fail(e)

  def raiseErrorR[R, A](e: Throwable): ZIO[R, Throwable, A] = ZIO.environment[R] >>> ZIO.fail(e)

}
