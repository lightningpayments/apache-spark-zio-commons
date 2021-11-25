package de.commons.lib.spark.testapps.sql101.app.logic.services

import de.commons.lib.spark.io.SparkDataFrameReader.{DataFrameQueryReader, DatabaseReader}
import de.commons.lib.spark.io.SparkDataFrameWriter.{DataFrameDatabaseWriter, DatabaseInsert}
import de.commons.lib.spark.services.Spark.HasSpark
import de.commons.lib.spark.services.SparkT
import de.commons.lib.spark.testapps.sql101.app.logic.joins.JoinedAgentsOrdersCustomers
import de.commons.lib.spark.testapps.sql101.app.logic.tables.{Agent, Customer, Order}
import org.apache.spark.sql.Dataset
import zio.ZIO

private[sql101] final class DbService(
    url: String,
    properties: java.util.Properties,
    sparkIO: ZIO[HasSpark, Throwable, SparkT]
) {

  private val databaseReader: DataFrameQueryReader = DatabaseReader(url, properties)
  private val databaseInsert: DataFrameDatabaseWriter = DatabaseInsert(url, properties)

  def getAgents: ZIO[HasSpark, Throwable, Dataset[Agent]] =
    sparkIO.flatMap(_.sparkWithLoggerM).map {
      case (spark, logger) =>
        logger.debug("select all agents")
        Agent.select(databaseReader)(spark)
    }

  def insertAgents(ds: Dataset[Agent]): ZIO[HasSpark, Throwable, Unit] =
    sparkIO
      .map(_.loggerM.map(_.debug("insert agents")))
      .map(_ => Agent.insert(ds)(databaseInsert))

  def getCustomers: ZIO[HasSpark, Throwable, Dataset[Customer]] =
    sparkIO.flatMap(_.sparkWithLoggerM).map {
      case (spark, logger) =>
        logger.debug("select all customers")
        Customer.select(databaseReader)(spark)
    }

  def getOrders: ZIO[HasSpark, Throwable, Dataset[Order]] =
    sparkIO.flatMap(_.sparkWithLoggerM).map {
      case (spark, logger) =>
        logger.debug("select all orders")
        Order.select(databaseReader)(spark)
    }

  def getAgentsStatistics: ZIO[HasSpark, Throwable, Dataset[JoinedAgentsOrdersCustomers]] =
    sparkIO.flatMap(_.sparkWithLoggerM).map {
      case (spark, logger) =>
        logger.debug("join agents via order via customers")
        JoinedAgentsOrdersCustomers.select(databaseReader)(spark)
    }

}
