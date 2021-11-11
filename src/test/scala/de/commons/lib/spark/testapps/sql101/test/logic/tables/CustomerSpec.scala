package de.commons.lib.spark.testapps.sql101.test.logic.tables

import de.commons.lib.spark.environments.io.SparkDataFrameReader
import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameQueryReader
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Customer
import de.commons.lib.spark.testapps.sql101.test.CreateTablesSupport
import de.commons.lib.spark.{MockDbTestSupport, SparkMySqlTestSupport, TestSpec}
import zio.ZIO

class CustomerSpec extends TestSpec with SparkMySqlTestSupport with MockDbTestSupport with CreateTablesSupport {

  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val customer =
    Customer("C00001", "Micheal", "New York", "New York", "USA", 2, 3000, 5000, 2000, 6000, "CCCC", "A008")

  "Customer#select" must {
    "return one customer dataset" in withSparkSession { implicit spark => _ =>
      mockDb(url, dbConf)(createTableCustomerQuery, customer.insert) {
        val reader: DataFrameQueryReader = SparkDataFrameReader.DatabaseReader(url, properties)
        whenReady(ZIO(Customer.select(reader)))(_.map(_.collect().toList) mustBe Right(customer :: Nil))
      }
    }
  }

}
