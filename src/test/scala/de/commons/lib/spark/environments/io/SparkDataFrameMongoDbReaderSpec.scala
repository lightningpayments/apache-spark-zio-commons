package de.commons.lib.spark.environments.io

import de.commons.lib.spark._
import de.commons.lib.spark.models.{CollectionName, DbName}
import org.apache.spark.sql.{Encoder, Encoders}
import org.bson.Document
import zio.ZIO

class SparkDataFrameMongoDbReaderSpec extends TestSpec with SparkMongoDbTestSupport with MockMongoDbTestSupport {

  private case class Agent(
      agentCode: String,
      agentName: String,
      workingArea: String,
      commission: String,
      phoneNo: String,
      country: Option[String]
  )

  private implicit val encoders: Encoder[Agent] = Encoders.product[Agent]

  private val json =
    """
      |{
      |  "agentCode": "A001",
      |  "agentName": "Alex",
      |  "workingArea": "London",
      |  "commission": "0.13",
      |  "phoneNo": "0001-111",
      |  "country": "England"
      |}
      |""".stripMargin

  private val agent = Agent("A001", "Alex", "London", "0.13", "0001-111", Some("England"))
  private val docs = Document.parse(json) :: Nil

  "SparkDataFrameReader#apply" must {
    "return an instance of agent" in withSparkSession { implicit spark => _ =>
      mockMongoDb(preparedDocs = docs)(dbName = "agents", collection = "agents", client = client) {
        lazy val reader =
          SparkDataFrameReader.MongoDbReader(properties)(DbName("agents"), CollectionName("agents"))
        whenReady(ZIO(reader.run.as[Agent])) {
          _.map(_.collect().toList) mustBe Right(agent :: Nil)
        }
      }
    }
  }

}
