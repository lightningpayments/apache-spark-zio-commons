package de.commons.lib.spark.testapps.sql101.app.logic.tables

import de.commons.lib.spark.io.SparkDataFrameReader.DataFrameQueryReader
import de.commons.lib.spark.io.SparkDataFrameWriter.DataFrameDatabaseWriter
import de.commons.lib.spark.models.{SqlQuery, TableName}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, when}

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

  private val tableName = TableName("agents")

  private val cols: List[String] =
    "agent_code" :: "agent_name" :: "working_area" :: "commission" :: "phone_no" :: "country" :: Nil

  private def columnAliases(implicit sparkSession: SparkSession): List[Column] = {
    import sparkSession.implicits._
    List(
      $"agent_code"   as "agentCode",
      $"agent_name"   as "agentName",
      $"working_area" as "workingArea",
      $"commission"   as "commission",
      $"phone_no"     as "phoneNo",
      $"country"      as "country"
    )
  }

  private val query: SqlQuery = SqlQuery(s"(SELECT ${cols.mkString(",")} FROM agents) as agents")

  // scalastyle:off
  def select(reader: DataFrameQueryReader)(implicit sparkSession: SparkSession): Dataset[Agent] = {
    import sparkSession.implicits._
    reader(query)
      .run
      .select(columnAliases: _*)
      .as[Agent]
      .withColumn(
        colName = "country",
        col = when(
          condition = $"country".isNull || $"country".isin("null"),
          value = lit(null).cast("string")
        ).otherwise($"country")
      ).as[Agent]
  }
  // scalastyle:on

  def insert(ds: Dataset[Agent])(writer: DataFrameDatabaseWriter): Unit =
    writer(tableName).run(ds.toDF(colNames = cols: _*))

}
