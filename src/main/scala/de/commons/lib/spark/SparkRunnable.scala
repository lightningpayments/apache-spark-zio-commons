package de.commons.lib.spark

import de.commons.lib.spark.environments.SparkRT
import de.commons.lib.spark.errors.SparkRunnableThrowable
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import zio.{Task, ZIO}

trait SparkRunnable[-R, A] {
  def run: ZIO[R, Throwable, A]
}

/**
 * You should use this for a smooth shutdown of the Spark session.
 */
object SparkRunnable {

  final case class RunnableSparkRT[-SR <: SparkRT, A](io: ZIO[SR, Throwable, A]) extends SparkRunnable[SR, A] {
    override def run: ZIO[SR, Throwable, A] =
      ZIO.environment[SR].flatMap(env =>
        ZIO.tupledPar(env.sparkM, env.loggerM).flatMap {
          case (session, logger) =>
            foldM[SR, A](io)(implicitly[SparkSession](session), implicitly[Logger](logger))
        }
      )
  }

  final case class RunnableR[-R, A](
      io: ZIO[R, Throwable, A])(
      implicit spark: SparkSession,
      logger: Logger
  ) extends SparkRunnable[R, A] {
    override def run: ZIO[R, Throwable, A] = foldM[R, A](io)
  }

  final case class Runnable[A](
      io: Task[A])(
      implicit spark: SparkSession,
      logger: Logger
  ) extends SparkRunnable[Any, A] {
    override def run: Task[A] = foldM[Any, A](io)
  }

  private def foldM[R, A](
    io: => ZIO[R, Throwable, A])(
    implicit spark: SparkSession,
    logger: Logger
  ): ZIO[R, Throwable, A] = io.foldM(
    failure = ex => Task(spark.stop()) >>> Task.fail(ex),
    success = a  => Task(spark.stop()) >>> Task.succeed(a)
  ).catchAll { ex =>
    Task(logger.error(ex.getMessage)) >>> Task.fail(SparkRunnableThrowable(ex))
  }

}
