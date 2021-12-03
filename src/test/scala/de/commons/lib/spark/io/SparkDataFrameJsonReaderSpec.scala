package de.commons.lib.spark.io

import de.commons.lib.spark._
import org.apache.spark.sql.{Encoder, Encoders}
import zio.ZIO

import java.nio.file.Paths

class SparkDataFrameJsonReaderSpec extends TestSpec with SparkTestSupport { self =>

  private case class Dummy(value: Long)
  private implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]

  "SparkDataFrameSqlReader#DatabaseReader" must {
    "return a json from filet" in withSparkSession { implicit spark => _ =>
      val path = Paths.get(self.getClass.getResource("/json/dummy.json").getPath)

      val reader = SparkDataFrameReader.JsonReaderPath(path.normalize().toString)
      val program = ZIO.tupled(
        ZIO(reader.run.as[Dummy].collect().toList),
        ZIO(spark.createDataset(Dummy(1) :: Nil).collect().toList)
      )

      whenReady(program) {
        case Left(ex) => fail(ex)
        case Right(tupled) => tupled._1 mustBe tupled._2
      }
    }
  }

}
