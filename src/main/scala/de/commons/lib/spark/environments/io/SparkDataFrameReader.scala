package de.commons.lib.spark.environments.io

import cats.implicits.showInterpolator
import de.commons.lib.spark.models.{CollectionName, DbName, SqlQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

trait SparkDataFrameReader {

  def sqlReader(
    sparkSession: SparkSession)(
    url: String,
    properties: Properties)(
    query: SqlQuery
  ): DataFrame = sparkSession.read.jdbc(url, show"$query", properties)

  def mongoDbReader(
    sparkSession: SparkSession,
    properties: Map[String, String])(
    database: DbName,
    collection: CollectionName
  ): DataFrame =
    sparkSession.read.format("com.mongodb.spark.sql.DefaultSource")
      .option("database", show"$database")
      .option("collection", show"$collection")
      .options(properties)
      .load()

}

object SparkDataFrameReader extends SparkDataFrameReader {
  type DataFrameQueryReader   = SqlQuery                 => DataFrame
  type DataFrameMongoDbReader = (DbName, CollectionName) => DataFrame
}
