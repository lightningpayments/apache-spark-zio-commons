package de.commons.lib.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import play.api.Configuration

import java.util.UUID

trait SparkMySqlTestSupport {

  protected implicit val logger: Logger = LogManager.getLogger(this.getClass)

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

  protected val appName: String = s"app_${UUID.randomUUID().toString}"
  protected val master: String = "local[*]"
  protected val sparkConf: Map[String, String] = configuration.get[Map[String, String]]("spark.config")
  protected val dbConf: Map[String, String] = configuration.get[Map[String, String]]("spark.db.config")

  private val spark: SparkSession = {
    val config = new SparkConf().setAll(sparkConf)
    val builder = SparkSession.builder().appName(appName).master(master).config(config)
    builder.getOrCreate()
  }

  def withSparkSession[A](f: SparkSession => Logger => A): A = f(spark)(logger)

}
