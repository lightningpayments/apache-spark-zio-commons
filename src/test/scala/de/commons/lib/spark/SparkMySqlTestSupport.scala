package de.commons.lib.spark

import ch.qos.logback.classic.LoggerContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import play.api.Configuration

import scala.util.Try

trait SparkMySqlTestSupport extends SparkTestSupport {

  protected implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    """
      |spark {
      |  config {}
      |  db {
      |    config {
      |      "url": "jdbc:h2:mem:testdb;MODE=MYSQL",
      |      "driver": "org.h2.Driver"
      |    }
      |  }
      |}
      |""".stripMargin))

  protected val sparkConf: Map[String, String] = configuration.get[Map[String, String]]("spark.config")
  protected val dbConf: Map[String, String] = configuration.get[Map[String, String]]("spark.db.config")

  override protected val spark: SparkSession = {
    val config = new SparkConf().setAll(sparkConf)
    val builder = SparkSession.builder().appName(appName).master(master).config(config)
    builder.getOrCreate()
  }

  Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
    ctx.stop()
    org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
  }

}
