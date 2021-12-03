package de.commons.lib.spark.io

import de.commons.lib.spark._
import org.apache.spark.sql.{Encoder, Encoders}
import zio.ZIO

import java.nio.file.Paths

class SparkDataFrameJsonReaderSpec extends TestSpec with SparkTestSupport { self =>

  private case class Dummy(value: Long)
  private implicit val encoders: Encoder[Dummy] = Encoders.product[Dummy]

  "SparkDataFrameSqlReader#JsonReaderPath" must {
    "return a json from filet" in withSparkSession { implicit spark => _ =>
      val path = Paths.get(self.getClass.getResource("/json/dummy.json").getPath)
      val reader = SparkDataFrameReader.JsonReaderPath(path.normalize().toString)

      whenReady(ZIO(reader.run.as[Dummy].collect().toList))(_ mustBe Right(Dummy(1) :: Nil))
    }
  }

  "SparkDataFrameSqlReader#JsonReaderDataset" must {
    "return a json from filet" in withSparkSession { implicit spark => _ =>
      import spark.implicits._

      val json = """[{"value": 1}]"""
      val ds = spark.createDataset(json :: Nil)
      val reader = SparkDataFrameReader.JsonReaderDataset(ds)

      whenReady(ZIO(reader.run.as[Dummy].collect().toList))(_ mustBe Right(Dummy(1) :: Nil))
    }
  }

}
