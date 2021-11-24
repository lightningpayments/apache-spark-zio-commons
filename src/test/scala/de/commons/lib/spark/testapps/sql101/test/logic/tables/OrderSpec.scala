package de.commons.lib.spark.testapps.sql101.test.logic.tables

import de.commons.lib.spark.io.SparkDataFrameReader
import de.commons.lib.spark.io.SparkDataFrameReader.DataFrameQueryReader
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Order
import de.commons.lib.spark.testapps.sql101.test.CreateTablesSupport
import de.commons.lib.spark.{MockDbTestSupport, SparkMySqlTestSupport, TestSpec}
import zio.ZIO

import java.time.LocalDate

class OrderSpec extends TestSpec with SparkMySqlTestSupport with MockDbTestSupport with CreateTablesSupport {

  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val order = Order(200100, 1000.00F, 600.00F, LocalDate.parse("2008-08-01"), "C00013", "A001", "SOD")

  "Order#select" must {
    "return one order dataset" in withSparkSession { implicit spark => _ =>
      mockDb(url, dbConf)(createTableOrdersQuery, order.insert) {
        val reader: DataFrameQueryReader = SparkDataFrameReader.DatabaseReader(url, properties)
        whenReady(ZIO(Order.select(reader)))(_.map(_.collect().toList) mustBe Right(order :: Nil))
      }
    }
  }

}
