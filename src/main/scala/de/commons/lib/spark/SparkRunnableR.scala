package de.commons.lib.spark

import de.commons.lib.spark.errors.SparkRunnableThrowable
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import zio.{Task, ZIO}

/**
 * You should use this for a smooth shutdown of the Spark session.
 */
object SparkRunnable {
  def apply[A](
    io: => Task[A])(
    implicit spark: SparkSession,
    logger: Logger
  ): ZIO[Any, Throwable, A] =
    io.foldM(
      failure = ex => Task(spark.stop()) >>> Task.fail(ex),
      success = a => Task(spark.stop()) >>> Task.succeed(a)
    ).catchAll { ex =>
      Task(logger.error(ex.getMessage)) >>> Task.fail(SparkRunnableThrowable(ex))
    }
}
