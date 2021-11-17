package de.commons.lib.spark.testapps.sql101.app.logic.services

import de.commons.lib.spark.environments.SparkR
import de.commons.lib.spark.environments.io.SparkDataFrameReader.{DataFrameQueryReader, DatabaseReader}
import de.commons.lib.spark.environments.io.SparkDataFrameWriter.{DataFrameDatabaseWriter, DatabaseInsert}
import de.commons.lib.spark.testapps.sql101.app.logic.joins.JoinedAgentsOrdersCustomers
import de.commons.lib.spark.testapps.sql101.app.logic.tables.{Agent, Customer, Order}
import org.apache.spark.sql.Dataset
import zio.ZIO

private[sql101] final class DbService(url: String, properties: java.util.Properties) {

  private val databaseReader: DataFrameQueryReader = DatabaseReader(url, properties)
  private val databaseInsert: DataFrameDatabaseWriter = DatabaseInsert(url, properties)

  def getAgents: ZIO[SparkR, Throwable, Dataset[Agent]] =
    for {
      env <- ZIO.environment[SparkR]
      _   <- env.loggerM.map(_.debug("select all agents"))
      ds  <- env.sparkM.map(implicit spark => Agent.select(databaseReader))
    } yield ds

  def insertAgents(ds: Dataset[Agent]): ZIO[SparkR, Throwable, Unit] =
    ZIO.environment[SparkR]
      .map(_.loggerM.map(_.debug("insert agents")))
      .map(_ => Agent.insert(ds)(databaseInsert))

  def getCustomers: ZIO[SparkR, Throwable, Dataset[Customer]] =
    for {
      env <- ZIO.environment[SparkR]
      _   <- env.loggerM.map(_.debug("select all customers"))
      ds  <- env.sparkM.map(implicit spark => Customer.select(databaseReader))
    } yield ds

  def getOrders: ZIO[SparkR, Throwable, Dataset[Order]] =
    for {
      env <- ZIO.environment[SparkR]
      _   <- env.loggerM.map(_.debug("select all orders"))
      ds  <- env.sparkM.map(implicit spark => Order.select(databaseReader))
    } yield ds

  def getAgentsStatistics: ZIO[SparkR, Throwable, Dataset[JoinedAgentsOrdersCustomers]] =
    for {
      env <- ZIO.environment[SparkR]
      _   <- env.loggerM.map(_.debug("join agents via order via customers"))
      ds  <- env.sparkM.map(implicit spark => JoinedAgentsOrdersCustomers.select(databaseReader))
    } yield ds

}
