package de.commons.lib.spark.testapps.sql101.app.logic.tables

import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameQueryReader
import de.commons.lib.spark.environments.io.SparkDataFrameWriter.DataFrameWriter
import de.commons.lib.spark.models.{SqlQuery, TableName}
import org.apache.spark.sql._

private[sql101] final case class Agent(
    agentCode: String,
    agentName: String,
    workingArea: String,
    commission: String,
    phoneNo: String,
    country: Option[String]
)

object Agent {

  implicit val encoders: Encoder[Agent] = Encoders.product[Agent]

  private val query: SqlQuery =
    SqlQuery("(SELECT agent_code, agent_name, working_area, commission, phone_no, country FROM agents) as agents")

  private val toDf: Dataset[Agent] => DataFrame = ds =>
    ds.toDF(colNames =
      "agent_code",
      "agent_name",
      "working_area",
      "commission",
      "phone_no",
      "country"
    )

  def select(reader: DataFrameQueryReader): Dataset[Agent] = {
    val df = reader(query)
    import df.sparkSession.implicits._
    df.select(cols =
      $"agent_code"   as "agentCode",
      $"agent_name"   as "agentName",
      $"working_area" as "workingArea",
      $"commission"   as "commission",
      $"phone_no"     as "phoneNo",
      $"country"      as "country"
    ).as[Agent]
  }

  def insert(ds: Dataset[Agent])(writer: DataFrameWriter): Unit =
    writer(toDf(ds), TableName("agents"))

}
