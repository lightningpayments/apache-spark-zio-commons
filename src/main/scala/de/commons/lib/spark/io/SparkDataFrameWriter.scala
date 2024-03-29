package de.commons.lib.spark.io

import cats.implicits.showInterpolator
import de.commons.lib.spark.models.TableName
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

object SparkDataFrameWriter {
  type DataFrameDatabaseWriter = TableName => DatabaseInsert

  sealed trait Writer extends Product with Serializable {
    def run(df: DataFrame): Unit
  }

  final case class DatabaseInsert(url: String, properties: Properties)(tableName: TableName) extends Writer {
    override def run(df: DataFrame): Unit =
      df.write.mode(SaveMode.Append).jdbc(url, show"$tableName", properties)
  }

  final case class DatabaseUpdate(url: String, properties: Properties)(tableName: TableName) extends Writer {
    override def run(df: DataFrame): Unit =
      df.write.mode(SaveMode.Overwrite).jdbc(url, show"$tableName", properties)
  }
}
