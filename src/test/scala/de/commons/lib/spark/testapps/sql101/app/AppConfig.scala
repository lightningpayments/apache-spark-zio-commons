package de.commons.lib.spark.testapps.sql101.app

import com.typesafe.config.ConfigFactory
import de.commons.lib.spark.SparkSessionLoader
import org.apache.log4j.{Level, Logger => Log4jLogger}
import play.api.Configuration

import java.util.Properties
import scala.jdk.CollectionConverters._

trait AppConfig {

  protected implicit val logger: Log4jLogger = Log4jLogger.getLogger(this.getClass)

  protected implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    """
      |spark {
      |  appName = "SimpleDbApp"
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

  protected val dbConf = configuration.get[Map[String, String]]("spark.db.config")

  protected val url = configuration.get[String]("spark.db.config.url")

  protected val properties = {
    val props = new Properties()
    props.putAll(dbConf.asJava)
    props
  }

  protected implicit val loader: SparkSessionLoader = new SparkSessionLoader(configuration)

}
