package de.commons.lib.spark.environments

import org.apache.log4j.Logger
import play.api.Configuration

import scala.language.higherKinds

class SparkR(override val configuration: Configuration, override val logger: Logger) extends SparkRT