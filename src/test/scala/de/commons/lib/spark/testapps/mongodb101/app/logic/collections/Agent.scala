package de.commons.lib.spark.testapps.mongodb101.app.logic.collections

import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.models.{CollectionName, DbName}
import org.apache.spark.sql.{Dataset, Encoder, Encoders}

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

  def getAll(reader: DataFrameMongoDbReader): Dataset[Agent] = {
    val df = reader(dbName, collectionName)
    import df.sparkSession.implicits._
    df.select(cols =
      $"AGENT_CODE"   as "agentCode",
      $"AGENT_NAME"   as "agentName",
      $"WORKING_AREA" as "workingArea",
      $"COMMISSION"   as "commission",
      $"PHONE_NO"     as "phoneNo",
      $"COUNTRY"      as "country"
    ).as[Agent]
  }

}