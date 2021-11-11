package de.commons.lib.spark.testapps.sql101.test.logic.tables

import de.commons.lib.spark.environments.io.SparkDataFrameReader
import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameQueryReader
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Agent
import de.commons.lib.spark.testapps.sql101.test.CreateTablesSupport
import de.commons.lib.spark.{MockDbTestSupport, SparkMySqlTestSupport, TestSpec}
import zio.ZIO

class AgentSpec extends TestSpec with SparkMySqlTestSupport with MockDbTestSupport with CreateTablesSupport {

  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val agent = Agent("A001", "Alex", "London", "0.13", "0001-111", Some("England"))

  "Agent#select" must {
    "return one agent dataset" in withSparkSession { implicit spark => _ =>
      mockDb(url, dbConf)(createTableAgentsQuery, agent.insert) {
        val reader: DataFrameQueryReader = SparkDataFrameReader.DatabaseReader(url, properties)
        whenReady(ZIO(Agent.select(reader)))(_.map(_.collect().toList) mustBe Right(agent :: Nil))
      }
    }
  }

}
