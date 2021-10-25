package de.commons.lib.spark.testapps.sql101.test.logic.tables

import de.commons.lib.spark.environments.io.SparkDbDataFrameReader
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Agent
import de.commons.lib.spark.testapps.sql101.test.CreateTablesSupport
import de.commons.lib.spark.{MockDbTestSupport, SparkTestSupport, TestSpec}
import zio.ZIO

class AgentSpec extends TestSpec with SparkTestSupport with MockDbTestSupport with CreateTablesSupport {

  private val url: String = "jdbc:h2:mem:testdb;MODE=MYSQL"
  private val agent = Agent("A001", "Alex", "London", "0.13", "0001-111", Some("England"))

  "Agent#select" must {
    "return one agent dataset" in withSparkSession { spark => logger =>
      mockDb(url, dbConf)(createTableAgentsQuery, agent.insert) {
        val program = (for {
          env    <- ZIO.environment[SparkDbDataFrameReader]
          reader  = env.reader(spark)(url, properties)(_)
          ds      = Agent.select(reader)
        } yield ds).provide(SparkDbDataFrameReader)

        whenReady(program)(_.map(_.collect().toList) mustBe Right(agent :: Nil))
      }
    }
  }

}
