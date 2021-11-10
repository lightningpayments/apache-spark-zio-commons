package de.commons.lib.spark.testapps.mongodb101.app

import com.typesafe.config.ConfigFactory
import de.commons.lib.spark.SparkSessionLoader
import org.apache.log4j.{Logger => Log4jLogger}
import play.api.Configuration

trait AppConfig {

  protected implicit val logger: Log4jLogger = Log4jLogger.getLogger(this.getClass)

  protected implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    """
      |spark {
      |  appName = "SimpleMongoDbApp"
      |  config {
      |    "spark.mongodb.input.uri": "mongodb://0.0.0.0:27017"
      |    "spark.mongodb.output.uri": "mongodb://0.0.0.0:27017"
      |  }
      |}
      |""".stripMargin))



  protected implicit val loader: SparkSessionLoader = new SparkSessionLoader(configuration)

}
