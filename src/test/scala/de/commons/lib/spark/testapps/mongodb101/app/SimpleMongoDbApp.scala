package de.commons.lib.spark.testapps.mongodb101.app

import cats.data.NonEmptyList
import de.commons.lib.spark.SparkRunnable.RunnableSparkRT
import de.commons.lib.spark.environments.SparkR
import de.commons.lib.spark.environments.io.SparkDataFrameReader
import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.testapps.mongodb101.app.logic.collections.Agent
import de.commons.lib.spark.testapps.mongodb101.app.logic.services.Service
import org.apache.spark.sql.Dataset
import zio.{ExitCode, URIO, ZIO}

private[testapps] object SimpleMongoDbApp extends zio.App with AppConfig {

  private val reader: DataFrameMongoDbReader = SparkDataFrameReader.MongoDbReader(properties)
  private val service: Service = new Service(reader)

  private val program: ZIO[SparkR, Throwable, Dataset[Agent]] =
    ZIO.environment[SparkR].>>=(_.sparkM.map(implicit spark => service.select(NonEmptyList.of("d756a"))))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    RunnableSparkRT[SparkR, Unit](program.map(_.show))
      .run
      .provide(new SparkR(configuration, logger))
      .exitCode

}
