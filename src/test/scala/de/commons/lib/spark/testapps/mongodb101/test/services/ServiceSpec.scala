package de.commons.lib.spark.testapps.mongodb101.test.services

import cats.data.NonEmptyList
import de.commons.lib.spark.environments.io.SparkDataFrameReader.{DataFrameMongoDbReader, MongoDbReader}
import de.commons.lib.spark.testapps.mongodb101.app.logic.collections.Agent
import de.commons.lib.spark.testapps.mongodb101.app.logic.services.Service
import de.commons.lib.spark.{MockMongoDbTestSupport, SparkMongoDbTestSupport, TestSpec}
import org.bson.Document
import zio.ZIO

class ServiceSpec extends TestSpec with SparkMongoDbTestSupport with MockMongoDbTestSupport {

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
  private val reader: DataFrameMongoDbReader = MongoDbReader(properties: Map[String, String])

  "Service#select" must {
    "return one agent dataset" in withSparkSession { implicit spark => _ =>
      mockMongoDb(preparedDocs = docs)(dbName = "agents", collection = "agents", client = client) {
        val service = new Service(reader)
        whenReady(ZIO(service.select(NonEmptyList.of("A001")))) {
          _.map(_.collect().toList) mustBe Right(agent :: Nil)
        }
      }
    }
  }

}
