package de.commons.lib.spark.testapps.mongodb101.test.collections

import de.commons.lib.spark.environments.io.SparkDataFrameReader
import de.commons.lib.spark.testapps.mongodb101.app.logic.collections.Agent
import de.commons.lib.spark.{MockMongoDbTestSupport, SparkMongoDbTestSupport, TestSpec}
import org.bson.Document
import zio.ZIO

class AgentSpec extends TestSpec with SparkMongoDbTestSupport with MockMongoDbTestSupport {

  private val json =
    """
      |{
      |  "AGENT_CODE": "A001",
      |  "AGENT_NAME": "Alex",
      |  "WORKING_AREA": "London",
      |  "COMMISSION": "0.13",
      |  "PHONE_NO": "0001-111",
      |  "COUNTRY": "England"
      |}
      |""".stripMargin

  private val agent = Agent("A001", "Alex", "London", "0.13", "0001-111", Some("England"))
  private val docs = Document.parse(json) :: Nil

  "Agent#getAll" must {
    "return one agent dataset" in withSparkSession { spark => _ =>
      mockDb(preparedDocs = docs)(dbName = "agents", collection = "agents", client = client) {
        val program = (for {
          env <- ZIO.environment[SparkDataFrameReader]
          ds   = Agent.getAll(env.mongoDbReader(spark))
        } yield ds).provide(SparkDataFrameReader)

        whenReady(program)(_.map(_.collect().toList) mustBe Right(agent :: Nil))
      }
    }
  }

}
