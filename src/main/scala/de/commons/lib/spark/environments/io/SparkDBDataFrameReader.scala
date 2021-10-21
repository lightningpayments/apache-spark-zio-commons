package de.commons.lib.spark.environments.io

import de.commons.lib.spark.models.SqlQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

trait SparkDBDataFrameReader {
  def reader(
    sparkSession: SparkSession)(
    url: String,
    properties: Properties)(
    query: SqlQuery
  ): DataFrame = sparkSession.read.jdbc(url, query.value, properties)
}

object SparkDBDataFrameReader extends SparkDBDataFrameReader {
  type DataFrameReader = SqlQuery => DataFrame
}
