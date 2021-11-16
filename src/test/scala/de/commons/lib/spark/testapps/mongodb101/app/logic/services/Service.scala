package de.commons.lib.spark.testapps.mongodb101.app.logic.services

import cats.data.NonEmptyList
import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.testapps.mongodb101.app.logic.collections.Agent
import org.apache.spark.sql.{Dataset, SparkSession}

import javax.inject.Inject

final class Service @Inject()(reader: DataFrameMongoDbReader) {

  def select(agentCodes: NonEmptyList[String])(implicit sparkSession: SparkSession): Dataset[Agent] =
    Agent.select(reader)(agentCodes)

}
