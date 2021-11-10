package de.commons.lib.spark.testapps.mongodb101.app

import de.commons.lib.spark.SparkRunnable.SparkRZIO
import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.SparkDataFrameReader
import de.commons.lib.spark.environments.io.SparkDataFrameReader.DataFrameMongoDbReader
import de.commons.lib.spark.testapps.mongodb101.app.logic.collections.Agent
import zio.{ExitCode, URIO, ZIO}

private[testapps] object SimpleMongoDbApp extends zio.App with AppConfig {

  private val readerIO: ZIO[SparkEnvironment with SparkDataFrameReader, Throwable, DataFrameMongoDbReader] =
    for {
      env <- ZIO.environment[SparkEnvironment with SparkDataFrameReader]
      df  <- env.sparkM.map(env.mongoDbReader)
    } yield df

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    SparkRZIO[SparkEnvironment, SparkDataFrameReader, Unit](readerIO.map(Agent.getAll).map(_.show))
      .run
      .provide(new SparkEnvironment(configuration, logger) with SparkDataFrameReader)
      .exitCode

}
