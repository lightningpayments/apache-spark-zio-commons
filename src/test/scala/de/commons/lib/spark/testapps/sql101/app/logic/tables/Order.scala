package de.commons.lib.spark.testapps.sql101.app.logic.tables

import de.commons.lib.spark.io.SparkDataFrameReader.DataFrameQueryReader
import de.commons.lib.spark.models.SqlQuery
import org.apache.spark.sql._
import org.apache.spark.sql.types.FloatType

import java.time.LocalDate

private[sql101] final case class Order(
    id: Int,
    amount: Float,
    advanceAmount: Float,
    orderDate: LocalDate,
    customerCode: String,
    agentCode: String,
    description: String
)

object Order {

  implicit val encoders: Encoder[Order] = Encoders.product[Order]

  private val query: SqlQuery = SqlQuery(
    """
      |(SELECT ord_num, ord_amount, advance_amount, ord_date, cust_code, agent_code, ord_description FROM orders)
      |as orders
      |""".stripMargin
  )

  def select(reader: DataFrameQueryReader)(implicit sparkSession: SparkSession): Dataset[Order] = {
    import sparkSession.implicits._
    reader(query).run.select(cols =
      $"ord_num"                        as "id",
      $"ord_amount".cast(FloatType)     as "amount",
      $"advance_amount".cast(FloatType) as "advanceAmount",
      $"ord_date"                       as "orderDate",
      $"cust_code"                      as "customerCode",
      $"agent_code"                     as "agentCode",
      $"ord_description"                as "description"
    ).as[Order]
  }

}
