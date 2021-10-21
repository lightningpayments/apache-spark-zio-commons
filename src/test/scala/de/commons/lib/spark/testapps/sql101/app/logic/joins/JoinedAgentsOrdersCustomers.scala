package de.commons.lib.spark.testapps.sql101.app.logic.joins

import de.commons.lib.spark.environments.io.SparkDBDataFrameReader.DataFrameReader
import de.commons.lib.spark.models.SqlQuery
import org.apache.spark.sql._
import org.apache.spark.sql.types.FloatType

import java.time.LocalDate

private[sql101] final case class JoinedAgentsOrdersCustomers(
    agentCode: String,
    agentName: String,
    workingArea: String,
    orderNumber: Int,
    orderAmount: Float,
    date: LocalDate
)

object JoinedAgentsOrdersCustomers {

  private implicit val encoders: Encoder[JoinedAgentsOrdersCustomers] = Encoders.product[JoinedAgentsOrdersCustomers]

  private val query: SqlQuery =
    SqlQuery(
      """
        |(
        |SELECT
        |  a.agent_code   as agent_code,
        |  a.agent_name   as agent_name,
        |  a.working_area as working_area,
        |  o.ord_num      as ord_num,
        |  o.ord_amount   as ord_amount,
        |  o.ord_date     as ord_date
        |FROM agents a
        |INNER JOIN orders   o ON a.agent_code   = o.agent_code
        |INNER JOIN customer c ON a.working_area = c.working_area AND a.agent_code = c.agent_code
        |ORDER BY ord_date ASC
        |) as joined_agents_orders_company
        |""".stripMargin)

  def select(reader: DataFrameReader): Dataset[JoinedAgentsOrdersCustomers] = {
    val df = reader(query)
    import df.sparkSession.implicits._
    df.select(cols =
      $"agent_code"                 as "agentCode",
      $"agent_name"                 as "agentName",
      $"working_area"               as "workingArea",
      $"ord_num"                    as "orderNumber",
      $"ord_amount".cast(FloatType) as "orderAmount",
      $"ord_date"                   as "date"
    ).as[JoinedAgentsOrdersCustomers]
  }

}
