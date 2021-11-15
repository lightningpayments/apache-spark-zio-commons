package de.commons.lib.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import play.api.Configuration

import java.util.UUID

final class SparkSessionLoader(configuration: Configuration) {

  private val appName: String =
    configuration.getOptional[String]("spark.appName").getOrElse(s"app_${UUID.randomUUID().toString}")

  private val master: String =
    configuration.getOptional[String]("spark.master").getOrElse("local[*]")

  private val sparkConfMap: Map[String, String] =
    configuration.getOptional[Map[String, String]]("spark.config").getOrElse(Map.empty)

  def getSpark: SparkSession = {
    val config = new SparkConf().setAll(sparkConfMap)
    val spark  = SparkSession.builder().appName(appName).master(master).config(config).getOrCreate()
    spark
  }

}
