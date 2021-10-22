package de.commons.lib.spark.testapps

import com.typesafe.config.ConfigFactory
import de.commons.lib.spark.SparkSessionLoader
import org.apache.log4j.{Logger => Log4jLogger}
import play.api.Configuration

trait AppConfig {

  protected implicit val logger: Log4jLogger = Log4jLogger.getLogger(this.getClass)

  implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    """
      |spark {
      |  db {
      |    config {
      |      "driver": "com.mysql.cj.jdbc.Driver"
      |      "url": "jdbc:mysql://localhost:3305/sparkdb"
      |      "user": "ronny"
      |      "password": "password"
      |    }
      |  }
      |}
      |""".stripMargin))

  implicit val loader: SparkSessionLoader = new SparkSessionLoader(configuration)

}
