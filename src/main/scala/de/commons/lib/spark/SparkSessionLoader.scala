package de.commons.lib.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import play.api.Configuration

final class SparkSessionLoader(configuration: Configuration) {
  private val appName   = configuration.get[String]("spark.appName")
  private val master    = configuration.get[String]("spark.master")
  private val sparkConf = configuration.get[Map[String, String]]("spark.config")

  def getSpark: SparkSession = {
    val config  = new SparkConf().setAll(sparkConf)
    val builder = SparkSession.builder().appName(appName).master(master).config(config)
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
