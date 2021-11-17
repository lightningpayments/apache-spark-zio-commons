package de.commons.lib.spark.testapps.sql101.app.logic.services

import de.commons.lib.spark.environments.SparkR
import de.commons.lib.spark.io.SparkDataFrameReader.{DataFrameQueryReader, DatabaseReader}
import de.commons.lib.spark.io.SparkDataFrameWriter.{DataFrameDatabaseWriter, DatabaseInsert}
import de.commons.lib.spark.testapps.sql101.app.logic.joins.JoinedAgentsOrdersCustomers
import de.commons.lib.spark.testapps.sql101.app.logic.tables.{Agent, Customer, Order}
import org.apache.spark.sql.Dataset
import zio.{RIO, ZIO}

private[sql101] final class DbService(url: String, properties: java.util.Properties) {

  private val databaseReader: DataFrameQueryReader = DatabaseReader(url, properties)
  private val databaseInsert: DataFrameDatabaseWriter = DatabaseInsert(url, properties)

  def getAgents: RIO[SparkR, Dataset[Agent]] =
    for {
      env <- RIO.environment[SparkR]
      _   <- env.loggerM.map(_.debug("select all agents"))
      ds  <- env.sparkM.map(implicit spark => Agent.select(databaseReader))
    } yield ds

  def insertAgents(ds: Dataset[Agent]): RIO[SparkR, Unit] =
    RIO.environment[SparkR]
      .map(_.loggerM.map(_.debug("insert agents")))
      .map(_ => Agent.insert(ds)(databaseInsert))

  def getCustomers: RIO[SparkR, Dataset[Customer]] =
    for {
      env <- RIO.environment[SparkR]
      _   <- env.loggerM.map(_.debug("select all customers"))
      ds  <- env.sparkM.map(implicit spark => Customer.select(databaseReader))
    } yield ds

  def getOrders: RIO[SparkR, Dataset[Order]] =
    for {
      env <- RIO.environment[SparkR]
      _   <- env.loggerM.map(_.debug("select all orders"))
      ds  <- env.sparkM.map(implicit spark => Order.select(databaseReader))
    } yield ds

  def getAgentsStatistics: RIO[SparkR, Dataset[JoinedAgentsOrdersCustomers]] =
    for {
      env <- RIO.environment[SparkR]
      _   <- env.loggerM.map(_.debug("join agents via order via customers"))
      ds  <- env.sparkM.map(implicit spark => JoinedAgentsOrdersCustomers.select(databaseReader))
    } yield ds

}
