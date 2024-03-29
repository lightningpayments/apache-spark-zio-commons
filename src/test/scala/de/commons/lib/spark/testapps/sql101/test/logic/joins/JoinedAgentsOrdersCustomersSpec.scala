package de.commons.lib.spark.testapps.sql101.test.logic.joins

import de.commons.lib.spark.io.SparkDataFrameReader
import de.commons.lib.spark.io.SparkDataFrameReader.DataFrameQueryReader
import de.commons.lib.spark.testapps.sql101.app.logic.joins.JoinedAgentsOrdersCustomers
import de.commons.lib.spark.testapps.sql101.app.logic.tables.{Agent, Customer, Order}
import de.commons.lib.spark.testapps.sql101.test.CreateTablesSupport
import de.commons.lib.spark.{MockDbTestSupport, SparkMySqlTestSupport, TestSpec}
import zio.ZIO

import java.time.LocalDate

class JoinedAgentsOrdersCustomersSpec
  extends TestSpec with SparkMySqlTestSupport with MockDbTestSupport with CreateTablesSupport {

  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val joined =
    JoinedAgentsOrdersCustomers("A001", "Alex", "London", 200100, 1000.00F, LocalDate.parse("2008-08-01"))
  private val agent =
    Agent("A001", "Alex", "London", "0.13", "0001-111", Some("England"))
  private val customer =
    Customer("C00001", "Alex", "London", "London", "USA", 2, 3000, 5000, 2000, 6000, "CCCC", "A001")
  private val order =
    Order(200100, 1000.00F, 600.00F, LocalDate.parse("2008-08-01"), "C00013", "A001", "SOD")

  "JoinedAgentsOrdersCustomers#select" must {
    "return one order dataset" in withSparkSession { implicit spark => _ =>
      mockDb(url, dbConf)(query =
        createTableOrdersQuery,
        createTableCustomerQuery,
        createTableAgentsQuery,
        order.insert,
        customer.insert,
        agent.insert
      ) {
        val reader: DataFrameQueryReader = SparkDataFrameReader.DatabaseReader(url, properties)
        whenReady(ZIO(JoinedAgentsOrdersCustomers.select(reader)))(
          _.map(_.collect().toList) mustBe Right(joined :: Nil)
        )
      }
    }
  }

}
