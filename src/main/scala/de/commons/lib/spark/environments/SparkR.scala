package de.commons.lib.spark.environments

import org.apache.log4j.Logger
import play.api.Configuration

import scala.language.higherKinds

trait SparkR extends Functions

object SparkR {
  class SparkEnvironment(override val configuration: Configuration, override val logger: Logger) extends SparkR
}
