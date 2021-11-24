package de.commons.lib.spark.services

import org.apache.log4j.Logger
import play.api.Configuration
import zio.{Has, Task, ULayer, ZIO, ZLayer}

import scala.language.higherKinds

object Spark {

  type HasSpark = Has[Service]

  trait Service {
    def apply(config: Configuration, logging: Logger): Task[SparkT]
  }

  val live: ULayer[HasSpark] = ZLayer.succeed {
    (config: Configuration, logging: Logger) => Task.succeed {
      new SparkT {
        override val configuration: Configuration = config
        override val logger: Logger = logging
      }
    }
  }

  def apply(implicit config: Configuration, logger: Logger): ZIO[HasSpark, Throwable, SparkT] =
    ZIO.accessM[HasSpark](_.get.apply(config, logger))

}
