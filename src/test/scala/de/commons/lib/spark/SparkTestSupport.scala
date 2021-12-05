package de.commons.lib.spark

import ch.qos.logback.classic.LoggerContext
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.util.Try

trait SparkTestSupport {

  protected implicit val logger: Logger = LogManager.getLogger(this.getClass)

  protected val appName: String = s"app_${UUID.randomUUID().toString}"
  protected val master: String = "local[*]"

  protected val spark: SparkSession = {
    val builder = SparkSession.builder().appName(appName).master(master)
    builder.getOrCreate()
  }

  def withSparkSession[A](f: SparkSession => Logger => A): A = f(spark)(logger)

  Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
    ctx.stop()
    org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
  }

}
