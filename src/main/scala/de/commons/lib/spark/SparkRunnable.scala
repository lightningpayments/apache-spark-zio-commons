package de.commons.lib.spark

import de.commons.lib.spark.environments.SparkRT
import de.commons.lib.spark.errors.SparkRunnableThrowable
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import zio.{RIO, Task, ZIO}

trait SparkRunnable[-R, A] {
  def run: RIO[R, A]
}

/**
 * You should use this for a smooth shutdown of the Spark session.
 */
object SparkRunnable {

  final case class RunnableSparkRT[-SR <: SparkRT, A](io: RIO[SR, A]) extends SparkRunnable[SR, A] {
    override def run: RIO[SR, A] =
      ZIO.environment[SR].flatMap(env =>
        ZIO.tupledPar(env.sparkM, env.loggerM).flatMap {
          case (session, logger) =>
            foldM[SR, A](io)(implicitly[SparkSession](session), implicitly[Logger](logger))
        }
      )
  }

  final case class RunnableR[-R, A](
      io: RIO[R, A])(
      implicit spark: SparkSession,
      logger: Logger
  ) extends SparkRunnable[R, A] {
    override def run: RIO[R, A] = foldM[R, A](io)
  }

  final case class Runnable[A](
      io: Task[A])(
      implicit spark: SparkSession,
      logger: Logger
  ) extends SparkRunnable[Any, A] {
    override def run: Task[A] = foldM[Any, A](io)
  }

  private def foldM[R, A](
    io: RIO[R, A])(
    implicit spark: SparkSession,
    logger: Logger
  ): RIO[R, A] = io.foldM(
    failure = ex => Task(spark.stop()) >>> Task.fail(ex),
    success = a  => Task(spark.stop()) >>> Task.succeed(a)
  ).catchAll { ex =>
    Task(logger.error(ex.getMessage)) >>> Task.fail(SparkRunnableThrowable(ex))
  }

}
