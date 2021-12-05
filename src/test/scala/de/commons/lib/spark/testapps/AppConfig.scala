package de.commons.lib.spark.testapps

import ch.qos.logback.classic.LoggerContext
import com.typesafe.config.ConfigFactory
import de.commons.lib.spark.SparkSessionLoader
import org.apache.log4j.{Logger => Log4jLogger}
import org.slf4j.LoggerFactory
import play.api.Configuration

import scala.util.Try

trait AppConfig {

  protected implicit val logger: Log4jLogger = Log4jLogger.getLogger(this.getClass)

  implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    """
      |spark {
      |  db {
      |    config {
      |      "driver": "com.mysql.cj.jdbc.Driver"
      |      "url": "jdbc:mysql://localhost:3306/sparkdb"
      |      "user": "ronny"
      |      "password": "password"
      |    }
      |  }
      |}
      |""".stripMargin))

  implicit val loader: SparkSessionLoader = new SparkSessionLoader(configuration)

  Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
    ctx.stop()
    org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
  }

}
