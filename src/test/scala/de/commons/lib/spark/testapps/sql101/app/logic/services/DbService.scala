package de.commons.lib.spark.testapps.sql101.app.logic.services

import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.SparkDataFrameReader.{DataFrameQueryReader, DatabaseReader}
import de.commons.lib.spark.environments.io.SparkDataFrameWriter.{DataFrameDatabaseWriter, DatabaseInsert}
import de.commons.lib.spark.testapps.sql101.app.logic.joins.JoinedAgentsOrdersCustomers
import de.commons.lib.spark.testapps.sql101.app.logic.tables.{Agent, Customer, Order}
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, Dataset}
import zio.ZIO

private[sql101] final class DbService(url: String, properties: java.util.Properties) {

  private val databaseReader: DataFrameQueryReader = DatabaseReader(url, properties)
  private val databaseInsert: DataFrameDatabaseWriter = DatabaseInsert(url, properties)

  // scalastyle:off
  def getAgents: ZIO[SparkEnvironment, Throwable, DataFrame] =
    for {
      env <- ZIO.environment[SparkEnvironment]
      _   <- env.loggerM.map(_.debug("select all agents"))
      df  <- env.sparkM.map { implicit spark =>
        import spark.implicits._
        Agent.select(databaseReader).withColumn(
          colName = "country",
          col     = when(
            condition = $"country".isNull || $"country".isin("null"),
            value     = lit(null).cast("string"))
        )
      }
    } yield df
  // scalastyle:on

  def insertAgents(ds: Dataset[Agent]): ZIO[SparkEnvironment, Throwable, Unit] =
    ZIO.environment[SparkEnvironment]
      .map(_.loggerM.map(_.debug("insert agents")))
      .map(_ => Agent.insert(ds)(databaseInsert))

  def getCustomers: ZIO[SparkEnvironment, Throwable, Dataset[Customer]] =
    for {
      env <- ZIO.environment[SparkEnvironment]
      _   <- env.loggerM.map(_.debug("select all customers"))
      ds  <- env.sparkM.map(implicit spark => Customer.select(databaseReader))
    } yield ds

  def getOrders: ZIO[SparkEnvironment, Throwable, Dataset[Order]] =
    for {
      env <- ZIO.environment[SparkEnvironment]
      _   <- env.loggerM.map(_.debug("select all orders"))
      ds  <- env.sparkM.map(implicit spark => Order.select(databaseReader))
    } yield ds

  def getAgentsStatistics: ZIO[SparkEnvironment, Throwable, Dataset[JoinedAgentsOrdersCustomers]] =
    for {
      env <- ZIO.environment[SparkEnvironment]
      _   <- env.loggerM.map(_.debug("join agents via order via customers"))
      ds  <- env.sparkM.map(implicit spark => JoinedAgentsOrdersCustomers.select(databaseReader))
    } yield ds

}
