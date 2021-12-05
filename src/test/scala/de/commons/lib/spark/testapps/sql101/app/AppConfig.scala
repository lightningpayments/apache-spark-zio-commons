package de.commons.lib.spark.testapps.sql101.app

import ch.qos.logback.classic.LoggerContext
import com.typesafe.config.ConfigFactory
import de.commons.lib.spark.SparkSessionLoader
import org.apache.log4j.{Logger => Log4jLogger}
import org.slf4j.LoggerFactory
import play.api.Configuration

import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.Try

trait AppConfig {

  protected implicit val logger: Log4jLogger = Log4jLogger.getLogger(this.getClass)

  protected implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    """
      |spark {
      |  appName = "SimpleDbApp"
      |  db {
      |    config {
      |      "driver": "com.mysql.cj.jdbc.Driver"
      |      "url": "jdbc:mysql://localhost:3306/sparkdb"
      |      "user": "root"
      |      "password": "password"
      |    }
      |  }
      |}
      |""".stripMargin))

  protected val dbConf = configuration.get[Map[String, String]]("spark.db.config")

  protected val url = configuration.get[String]("spark.db.config.url")

  protected val properties = {
    val props = new Properties()
    props.putAll(dbConf.asJava)
    props
  }

  protected implicit val loader: SparkSessionLoader = new SparkSessionLoader(configuration)

  Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
    ctx.stop()
    org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
  }

}
