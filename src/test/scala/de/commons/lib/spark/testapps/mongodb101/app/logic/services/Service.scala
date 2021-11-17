package de.commons.lib.spark.testapps.mongodb101.app.logic.services

import cats.data.NonEmptyList
import de.commons.lib.spark.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.testapps.mongodb101.app.logic.collections.Agent
import org.apache.spark.sql.{Dataset, SparkSession}

final class Service(reader: DataFrameMongoDbReader) {

  def select(agentCodes: NonEmptyList[String])(implicit sparkSession: SparkSession): Dataset[Agent] =
    Agent.select(reader)(agentCodes)

}
