package de.commons.lib.spark.testapps.mongodb101.app.logic.collections

import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.models.{CollectionName, DbName}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

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

  def getAll(reader: DataFrameMongoDbReader)(implicit sparkSession: SparkSession): Dataset[Agent] = {
    import sparkSession.implicits._

    reader(dbName, collectionName).run.select(cols =
      $"AGENT_CODE"   as "agentCode",
      $"AGENT_NAME"   as "agentName",
      $"WORKING_AREA" as "workingArea",
      $"COMMISSION"   as "commission",
      $"PHONE_NO"     as "phoneNo",
      $"COUNTRY"      as "country"
    ).as[Agent]
  }

}
