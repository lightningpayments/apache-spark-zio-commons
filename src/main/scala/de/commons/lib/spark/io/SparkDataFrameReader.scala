package de.commons.lib.spark.io

import cats.implicits.showInterpolator
import de.commons.lib.spark.models.{CollectionName, DbName, SqlQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.Properties

object SparkDataFrameReader {
  type DataFrameQueryReader   = SqlQuery                 => DatabaseReader
  type DataFrameMongoDbReader = (DbName, CollectionName) => MongoDbReader

  sealed trait Reader extends Product with Serializable {
    def run(implicit sparkSession: SparkSession): DataFrame
  }

  final case class DatabaseReader(url: String, properties: Properties)(query: SqlQuery) extends Reader {
    override def run(implicit sparkSession: SparkSession): DataFrame = {
      sparkSession.read.jdbc(url, show"$query", properties)
    }
  }

  final case class MongoDbReader(properties: Map[String, String])(dbName: DbName, collectionName: CollectionName)
    extends Reader {
    override def run(implicit sparkSession: SparkSession): DataFrame =
      sparkSession.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("database", show"$dbName")
        .option("collection", show"$collectionName")
        .options(properties)
        .load()
  }

  final case class JsonReaderPath(path: String) extends Reader {
    override def run(implicit sparkSession: SparkSession): DataFrame =
      sparkSession.read.option("multiline", value = true).json(path)
  }

  final case class JsonReaderDataset(ds: Dataset[String]) extends Reader {
    override def run(implicit sparkSession: SparkSession): DataFrame =
      sparkSession.read.json(ds)
  }
}
