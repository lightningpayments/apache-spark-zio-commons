package de.commons.lib.spark.testapps.sql101.app

import de.commons.lib.spark.services.Spark.HasSpark
import de.commons.lib.spark.services.{Spark, SparkT}
import de.commons.lib.spark.testapps.sql101.app.logic.services.DbService
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Agent
import de.commons.lib.spark.testapps.sql101.app.logic.tables.Agent.encoders
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, udf}
import zio.{ExitCode, Task, URIO, ZIO}

import java.util.UUID
import scala.language.postfixOps

private[testapps] object SimpleDbApp extends zio.App with AppConfig {

  private val sparkIO: ZIO[HasSpark, Throwable, SparkT] = Spark.apply(configuration, logger)
  private val dbService = new DbService(url, properties, sparkIO)

  private def programInsertAgents(ds: Dataset[Agent]): ZIO[HasSpark, Throwable, Unit] = {
    val agentCode = "agentCode"
    val takeRandomAgentCode = udf((_: String) => UUID.randomUUID().toString.take(5))
    val agents = Task(ds.withColumn(agentCode, takeRandomAgentCode(col(agentCode))).as[Agent].sort(agentCode))
    agents >>= dbService.insertAgents
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val io: ZIO[HasSpark, Throwable, Unit] = for {
      agentsDf <- dbService.getAgents
      _        <- Task(agentsDf.show)
      _        <- dbService.getCustomers.map(_.show())
      _        <- dbService.getOrders.map(_.show())
      _        <- dbService.getAgentsStatistics.map(_.show())
      _        <- programInsertAgents(agentsDf.as[Agent])
      _        <- dbService.getAgents.map(_.show())
    } yield ()

    io.provideLayer(Spark.live).exitCode
  }

}
