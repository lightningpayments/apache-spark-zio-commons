package de.commons.lib.spark

import de.commons.lib.spark.environments.SparkR
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

  final case class SparkRZIO[-SR <: SparkR, A](io: ZIO[SR, Throwable, A])
    extends SparkRunnable[SR, A] {
    override def run: ZIO[SR, Throwable, A] =
      for {
        (spark, logger) <- ZIO.tupledPar(
          zio1 = ZIO.environment[SR].>>=(_.sparkM),
          zio2 = ZIO.environment[SR].>>=(_.loggerM)
        )
        a <- foldM[SR, A](io)(implicitly[SparkSession](spark), implicitly[Logger](logger))
      } yield a
  }

  final case class SparkZIO[-R, A](
      io: ZIO[R, Throwable, A])(
      implicit spark: SparkSession,
      logger: Logger
  ) extends SparkRunnable[R, A] {
    override def run: ZIO[R, Throwable, A] = foldM[R, A](io)
  }

  final case class SparkTask[A](
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
