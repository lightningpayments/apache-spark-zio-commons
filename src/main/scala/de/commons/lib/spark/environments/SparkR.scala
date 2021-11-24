package de.commons.lib.spark.environments

import org.apache.log4j.Logger
import play.api.Configuration
import zio.{Has, Task, ULayer, ZIO, ZLayer}

import scala.language.higherKinds

object Spark {

  type HasSpark = Has[Service]

  trait Service {
    def apply(config: Configuration, logging: Logger): Task[SparkRT]
  }

  val live: ULayer[HasSpark] = ZLayer.succeed {
    (config: Configuration, logging: Logger) => Task.succeed {
      new SparkRT {
        override val configuration: Configuration = config
        override val logger: Logger = logging
      }
    }
  }

  def apply(implicit config: Configuration, logger: Logger): ZIO[HasSpark, Throwable, SparkRT] =
    ZIO.accessM[HasSpark](_.get.apply(config, logger))

}
