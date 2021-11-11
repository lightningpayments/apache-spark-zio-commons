package de.commons.lib.spark.testapps.mongodb101.app

import de.commons.lib.spark.SparkRunnable.SparkRZIO
import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.SparkDataFrameReader
import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.testapps.mongodb101.app.logic.collections.Agent
import org.apache.spark.sql.Dataset
import zio.{ExitCode, URIO, ZIO}

private[testapps] object SimpleMongoDbApp extends zio.App with AppConfig {

  private val program: ZIO[SparkEnvironment with SparkDataFrameReader, Throwable, Dataset[Agent]] =
    ZIO.environment[SparkEnvironment with SparkDataFrameReader].flatMap { env =>
      val reader: DataFrameMongoDbReader = SparkDataFrameReader.MongoDbReader(properties = properties, _, _)
      env.sparkM.map(implicit spark => Agent.getAll(reader))
    }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    SparkRZIO[SparkEnvironment, SparkDataFrameReader, Unit](program.map(_.show))
      .run
      .provide(new SparkEnvironment(configuration, logger) with SparkDataFrameReader)
      .exitCode

}
