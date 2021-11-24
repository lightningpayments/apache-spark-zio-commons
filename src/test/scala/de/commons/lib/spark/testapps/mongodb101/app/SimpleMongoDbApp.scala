package de.commons.lib.spark.testapps.mongodb101.app

import cats.data.NonEmptyList
import de.commons.lib.spark.io.SparkDataFrameReader
import de.commons.lib.spark.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.services.Spark
import de.commons.lib.spark.testapps.mongodb101.app.logic.services.Service
import zio.{ExitCode, URIO}

private[testapps] object SimpleMongoDbApp extends zio.App with AppConfig {

  private val reader: DataFrameMongoDbReader = SparkDataFrameReader.MongoDbReader(properties)
  private val service: Service = new Service(reader)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    Spark.apply.provideLayer(Spark.live)
      .flatMap(_.sparkM)
      .map(implicit spark => service.select(NonEmptyList.of("d756a")))
      .exitCode

}
