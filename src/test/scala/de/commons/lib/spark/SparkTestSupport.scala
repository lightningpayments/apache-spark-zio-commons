package de.commons.lib.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import play.api.Configuration

trait SparkTestSupport {

  protected implicit val logger: Logger = LogManager.getLogger(this.getClass)

  protected implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    """
      |spark {
      |  appName = "TestSpec"
      |  master = "local[*]"
      |  config {}
      |  db {
      |    config {
      |      "url": "jdbc:h2:mem:testdb;MODE=MYSQL",
      |      "driver": "org.h2.Driver"
      |    }
      |  }
      |}
      |""".stripMargin))

  protected val appName: String = configuration.get[String]("spark.appName")
  protected val master: String = configuration.get[String]("spark.master")
  protected val sparkConf: Map[String, String] = configuration.get[Map[String, String]]("spark.config")
  protected val dbConf: Map[String, String] = configuration.get[Map[String, String]]("spark.db.config")

  private lazy val spark: SparkSession = {
    val config = new SparkConf().setAll(sparkConf)
    val builder = SparkSession.builder().appName(appName).master(master).config(config)
    builder.getOrCreate()
  }

  def withSparkSession[A, T](f: SparkSession => Logger => T): T = f(spark)(logger)
}
