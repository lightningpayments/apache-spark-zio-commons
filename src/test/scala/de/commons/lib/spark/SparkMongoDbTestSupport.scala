package de.commons.lib.spark

import com.mongodb.client.{MongoClient, MongoClients}
import com.typesafe.config.ConfigFactory
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import play.api.Configuration

import java.net.InetSocketAddress
import java.util.UUID

trait SparkMongoDbTestSupport {

  protected implicit val logger: Logger = LogManager.getLogger(this.getClass)

  private val server = new MongoServer(new MemoryBackend())

  private val iSockAddress: InetSocketAddress = server.bind()

  private val mongoDbUrl = s"mongodb://${iSockAddress.getHostName}:${server.getLocalAddress.getPort}"

  protected implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    s"""
      |spark {
      |  config {}
      |  config {
      |    "spark.mongodb.input.uri": "$mongoDbUrl"
      |    "spark.mongodb.output.uri": "$mongoDbUrl"
      |    "spark.mongodb.input.partitioner": "MongoPaginateBySizePartitioner"
      |    "spark.mongodb.input.partitionerOptions.partitionSizeMB": "64"
      |  }
      |}
      |""".stripMargin))

  protected val appName: String = s"app_${UUID.randomUUID().toString}"
  protected val master: String = "local[*]"
  protected val sparkConf: Map[String, String] = configuration.get[Map[String, String]]("spark.config")

  private lazy val spark: SparkSession = {
    val config = new SparkConf().setAll(sparkConf)
    val builder = SparkSession.builder().appName(appName).master(master).config(config)
    builder.getOrCreate()
  }

  protected val client: MongoClient = MongoClients.create(mongoDbUrl)

  def withSparkSession[A, T](f: SparkSession => Logger => T): T = f(spark)(logger)
}
