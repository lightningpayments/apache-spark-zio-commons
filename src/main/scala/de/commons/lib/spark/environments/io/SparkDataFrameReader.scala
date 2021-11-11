package de.commons.lib.spark.environments.io

import cats.implicits.showInterpolator
import de.commons.lib.spark.models.{CollectionName, DbName, SqlQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

trait SparkDataFrameReader {

  sealed trait Reader extends Product with Serializable {
    def run(implicit sparkSession: SparkSession): DataFrame
  }

  final case class DatabaseReader(url: String, properties: Properties, query: SqlQuery) extends Reader {
    override def run(implicit sparkSession: SparkSession): DataFrame =
      sparkSession.read.jdbc(url, show"$query", properties)
  }

  final case class MongoDbReader(properties: Map[String, String], dbName: DbName, collectionName: CollectionName)
    extends Reader {
    override def run(implicit sparkSession: SparkSession): DataFrame =
      sparkSession.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("database", show"$dbName")
        .option("collection", show"$collectionName")
        .options(properties)
        .load()
  }

}

object SparkDataFrameReader extends SparkDataFrameReader {
  type DataFrameQueryReader   = SqlQuery                 => DatabaseReader
  type DataFrameMongoDbReader = (DbName, CollectionName) => MongoDbReader
}
