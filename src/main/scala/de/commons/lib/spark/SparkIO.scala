package de.commons.lib.spark

import de.commons.lib.spark.environments.SparkR
import de.commons.lib.spark.errors.SparkRunnableThrowable
import zio.{Task, ZIO}

/**
 * You should use this for a smooth shutdown of the Spark session.
 * R1 as customize environment derived from {{{ SparkR }}}
 */
final case class SparkIO[R1 <: SparkR, R2, A](io: ZIO[R1 with R2, Throwable, A]) {

  def run: ZIO[R1 with R2, Throwable, A] =
    ZIO.environment[R1 with R2].>>=(_.sparkM).>>= { spark =>
      io.foldM(
        failure = throwable => Task(spark.stop()) >>> Task.fail(throwable),
        success = a => Task(spark.stop()) >>> Task.succeed(a)
      )
    }.catchAll { throwable =>
      ZIO.environment[R1 with R2].>>=(_.loggerM.map(_.error(throwable.getMessage))) >>>
      Task.fail(SparkRunnableThrowable(throwable))
    }

}
