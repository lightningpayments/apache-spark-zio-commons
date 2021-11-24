package de.commons.lib.spark.services

import cats.Functor
import de.commons.lib.spark.SparkSessionLoader
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import play.api.Configuration
import zio.{Task, ZIO}

import scala.language.{higherKinds, postfixOps}
import scala.util.Try

trait SparkT {

  val configuration: Configuration
  val logger: Logger

  val sparkM: Task[SparkSession] =
    for {
      loader <- Task(new SparkSessionLoader(configuration))
      spark  <- Task.fromTry(Try(loader.getSpark))
    } yield spark

  val loggerM: Task[Logger] = Task.succeed(logger)

  val unit: Task[Unit] = Task.unit

  def apply[A](f: (SparkSession, Logger) => A): Task[A] = ZIO.tupledPar(sparkM, loggerM).map(f tupled)

  def applyR[R, A](ff: => ZIO[R, Throwable, (SparkSession, Logger) => A]): ZIO[R, Throwable, A] =
    ZIO.tupledPar(sparkM, loggerM).flatMap {
      case (session, logger) => ff.map(_(session, logger))
    }

  def lift[F[_], A, B](f: (SparkSession, Logger) => A => B)(fa: => F[A])(implicit M: Functor[F]): Task[F[B]] =
    ZIO.tupledPar(sparkM, loggerM).flatMap {
      case (session, logger) => Task(M.fmap(fa)(f(session, logger)))
    }

  def liftR[R, F[_], A, B](
    f: (SparkSession, Logger) => A => B)(
    fa: => F[A])(
    implicit M: Functor[F]
  ): ZIO[R, Throwable, F[B]] =
    ZIO.environment[R] >>> lift(f)(fa)

  def raiseError[A](e: Throwable): Task[A] = Task.fail(e)

  def raiseErrorR[R, A](e: Throwable): ZIO[R, Throwable, A] = ZIO.environment[R] >>> ZIO.fail(e)

}