package de.commons.lib.spark

import ch.qos.logback.classic.LoggerContext
import com.mongodb.client.{MongoClient, MongoClients}
import com.typesafe.config.ConfigFactory
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import play.api.Configuration

import java.net.InetSocketAddress
import scala.util.Try

trait SparkMongoDbTestSupport extends SparkTestSupport {

  private val server = new MongoServer(new MemoryBackend())

  private val iSockAddress: InetSocketAddress = server.bind()

  protected val mongoDbUrl = s"mongodb://${iSockAddress.getHostName}:${server.getLocalAddress.getPort}"

  protected val properties = Map(
    "uri" -> mongoDbUrl,
    "partitioner" -> "MongoPaginateBySizePartitioner",
    "partitionerOptions.partitionSizeMB" -> "64"
  )

  protected implicit val configuration: Configuration = Configuration(ConfigFactory.parseString(
    s"""
      |spark {
      |  config {
      |    "spark.mongodb.input.uri": "$mongoDbUrl",
      |    "spark.mongodb.output.uri": "$mongoDbUrl",
      |    "spark.mongodb.input.partitioner": "MongoPaginateBySizePartitioner",
      |    "spark.mongodb.input.partitionerOptions.partitionSizeMB": "64"
      |  }
      |}
      |""".stripMargin))

  protected val sparkConf: Map[String, String] = configuration.get[Map[String, String]]("spark.config")

  override protected val spark: SparkSession = {
    val config = new SparkConf().setAll(sparkConf)
    val builder = SparkSession.builder().appName(appName).master(master).config(config)
    builder.getOrCreate()
  }

  protected val client: MongoClient = MongoClients.create(mongoDbUrl)

  Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
    ctx.stop()
    org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
  }

}
