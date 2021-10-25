package de.commons.lib.spark.testapps.sql101.app.logic.tables

import de.commons.lib.spark.environments.io.SparkDbDataFrameReader.DataFrameReader
import de.commons.lib.spark.models.SqlQuery
import org.apache.spark.sql._
import org.apache.spark.sql.types.FloatType

private[sql101] final case class Customer(
    code: String,
    name: String,
    city: String,
    workingArea: String,
    country: String,
    grade: Int,
    openingAmt: Float,
    receiveAmt: Float,
    paymentAmt: Float,
    outstandingAmt: Float,
    phone: String,
    agentCode: String
)

object Customer {
  private implicit val encoders: Encoder[Customer] = Encoders.product[Customer]

  private val query: SqlQuery = SqlQuery(
    """
      |(
      | SELECT
      |  cust_code, cust_name, cust_city,
      |  working_area, cust_country, grade,
      |  opening_amt, receive_amt, payment_amt,
      |  outstanding_amt, phone_no, agent_code
      |FROM customer
      |) as customer
      |""".stripMargin
  )

  def select(reader: DataFrameReader): Dataset[Customer] = {
    val df = reader(query)
    import df.sparkSession.implicits._
    df.select(cols =
      $"cust_code"                       as "code",
      $"cust_name"                       as "name",
      $"cust_city"                       as "city",
      $"working_area"                    as "workingArea",
      $"cust_country"                    as "country",
      $"grade"                           as "grade",
      $"opening_amt".cast(FloatType)     as "openingAmt",
      $"receive_amt".cast(FloatType)     as "receiveAmt",
      $"payment_amt".cast(FloatType)     as "paymentAmt",
      $"outstanding_amt".cast(FloatType) as "outstandingAmt",
      $"phone_no"                        as "phone",
      $"agent_code"                      as "agentCode"
    ).as[Customer]
  }

}
