package de.commons.lib.spark.environments.io

import cats.implicits.showInterpolator
import de.commons.lib.spark.environments.io.SparkDataFrameWriter.DataFrameWriter
import de.commons.lib.spark.models.TableName
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

trait SparkDataFrameWriter {
  def insert(url: String, properties: Properties): DataFrameWriter = (df, table) =>
    df.write.mode(SaveMode.Append).jdbc(url, show"$table", properties)

  def update(url: String, properties: Properties): DataFrameWriter = (df, table) =>
    df.write.mode(SaveMode.Overwrite).jdbc(url, show"$table", properties)
}

object SparkDataFrameWriter extends SparkDataFrameWriter {
  type DataFrameWriter = (DataFrame, TableName) => Unit
}
