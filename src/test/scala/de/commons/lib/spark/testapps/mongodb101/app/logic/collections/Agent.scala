package de.commons.lib.spark.testapps.mongodb101.app.logic.collections

import cats.data.NonEmptyList
import de.commons.lib.spark.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.models.{CollectionName, DbName}
import org.apache.spark.sql.{Column, Dataset, Encoder, Encoders, SparkSession}

private[mongodb101] final case class Agent(
    agentCode: String,
    agentName: String,
    workingArea: String,
    commission: String,
    phoneNo: String,
    country: Option[String]
)

object Agent {

  implicit val encoders: Encoder[Agent] = Encoders.product[Agent]

  private val collectionName = CollectionName("agents")
  private val dbName = DbName("agents")

  private def cols(implicit sparkSession: SparkSession): List[Column] = {
    import sparkSession.implicits._
    List(
      $"AGENT_CODE"   as "agentCode",
      $"AGENT_NAME"   as "agentName",
      $"WORKING_AREA" as "workingArea",
      $"COMMISSION"   as "commission",
      $"PHONE_NO"     as "phoneNo",
      $"COUNTRY"      as "country"
    )
  }

  private def getAll(reader: DataFrameMongoDbReader)(implicit sparkSession: SparkSession): Dataset[Agent] =
    reader(dbName, collectionName).run.select(cols: _*).as[Agent]

  def select(
    reader: DataFrameMongoDbReader)(
    agentCodes: NonEmptyList[String])(
    implicit sparkSession: SparkSession
  ): Dataset[Agent] = {
    import sparkSession.implicits._
    getAll(reader).filter($"agentCode".isin(agentCodes.toList: _*))
  }

}
