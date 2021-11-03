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

  final case class SparkRZIO[-R1 <: SparkR, -R2, A](io: ZIO[R1 with R2, Throwable, A])
    extends SparkRunnable[R1 with R2, A] {
    override def run: ZIO[R1 with R2, Throwable, A] =
      for {
        (spark, logger) <- ZIO.tupledPar(
          zio1 = ZIO.environment[R1 with R2].>>=(_.sparkM),
          zio2 = ZIO.environment[R1 with R2].>>=(_.loggerM)
        )
        a <- foldM[R1 with R2, A](io)(implicitly[SparkSession](spark), implicitly[Logger](logger))
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
    io: ZIO[R, Throwable, A])(
    implicit spark: SparkSession,
    logger: Logger
  ): ZIO[R, Throwable, A] = io.foldM(
    failure = throwable => Task(spark.stop()) >>> Task.fail(throwable),
    success = a         => Task(spark.stop()) >>> Task.succeed(a)
  ).catchAll { throwable =>
    Task(logger.error(throwable.getMessage)) >>> Task.fail(SparkRunnableThrowable(throwable))
  }

}
