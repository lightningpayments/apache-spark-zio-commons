package de.commons.lib.spark.testapps.mongodb101.app

import com.typesafe.config.ConfigFactory
import de.commons.lib.spark.SparkSessionLoader
import org.apache.log4j.{Logger => Log4jLogger}
import play.api.Configuration

trait AppConfig {

  protected implicit val logger: Log4jLogger = Log4jLogger.getLogger(this.getClass)

  protected val uri = "mongodb://0.0.0.0:27017"

  protected val properties = Map(
    "uri" -> uri,
    "partitioner" -> "MongoPaginateBySizePartitioner",
    "partitionerOptions.partitionSizeMB" -> "64"
  )

  protected implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    s"""
      |spark {
      |  appName = "SimpleMongoDbApp"
      |  config {
      |    "spark.mongodb.input.uri": "$uri"
      |    "spark.mongodb.output.uri": "$uri"
      |  }
      |}
      |""".stripMargin))

  protected implicit val loader: SparkSessionLoader = new SparkSessionLoader(configuration)

}
