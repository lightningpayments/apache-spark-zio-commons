package de.commons.lib.spark.testapps.sql101.test

import de.commons.lib.spark.testapps.sql101.app.logic.tables.{Agent, Customer, Order}

trait CreateTablesSupport {

  implicit class RichAgent(agent: Agent) {
    val insert: String =
      s"""
         |INSERT INTO `agents` VALUES
         |(
         |'${agent.agentCode}',
         |'${agent.agentName}',
         |'${agent.workingArea}',
         |'${agent.commission}',
         |'${agent.phoneNo}',
         |'${agent.country.getOrElse("null")}'
         |);
         |""".stripMargin
  }

  val createTableAgentsQuery: String =
    """
      |CREATE TABLE IF NOT EXISTS `agents` (
      |  `AGENT_CODE` varchar(6) NOT NULL,
      |  `AGENT_NAME` varchar(40) DEFAULT NULL,
      |  `WORKING_AREA` varchar(35) DEFAULT NULL,
      |  `COMMISSION` decimal(10,2) DEFAULT NULL,
      |  `PHONE_NO` varchar(15) DEFAULT NULL,
      |  `COUNTRY` varchar(25) DEFAULT NULL
      |) ENGINE=InnoDB;
      |""".stripMargin

  implicit class RichOrder(agent: Order) {
    val insert: String =
      s"""
         |INSERT INTO `orders` VALUES
         |(
         |'${agent.id}',
         |'${agent.amount}',
         |'${agent.advanceAmount}',
         |'${agent.orderDate}',
         |'${agent.customerCode}',
         |'${agent.agentCode}',
         |'${agent.description}'
         |);
         |""".stripMargin
  }

  val createTableOrdersQuery: String =
    """
      |CREATE TABLE IF NOT EXISTS `orders` (
      |  `ORD_NUM` decimal(6,0) NOT NULL,
      |  `ORD_AMOUNT` decimal(12,2) NOT NULL,
      |  `ADVANCE_AMOUNT` decimal(12,2) NOT NULL,
      |  `ORD_DATE` date NOT NULL,
      |  `CUST_CODE` varchar(6) NOT NULL,
      |  `AGENT_CODE` char(6) NOT NULL,
      |  `ORD_DESCRIPTION` varchar(60) NOT NULL
      |) ENGINE=InnoDB;
      |""".stripMargin

  implicit class RichCustomer(customer: Customer) {
    val insert: String =
      s"""
         |INSERT INTO `customer` VALUES
         |(
         |'${customer.code}',
         |'${customer.name}',
         |'${customer.city}',
         |'${customer.workingArea}',
         |'${customer.country}',
         |'${customer.grade}',
         |'${customer.openingAmt}',
         |'${customer.receiveAmt}',
         |'${customer.paymentAmt}',
         |'${customer.outstandingAmt}',
         |'${customer.phone}',
         |'${customer.agentCode}'
         |);
         |""".stripMargin
  }

  val createTableCustomerQuery: String =
    """
      |CREATE TABLE IF NOT EXISTS `customer` (
      |  `CUST_CODE` varchar(6) NOT NULL,
      |  `CUST_NAME` varchar(40) NOT NULL,
      |  `CUST_CITY` varchar(35) DEFAULT NULL,
      |  `WORKING_AREA` varchar(35) NOT NULL,
      |  `CUST_COUNTRY` varchar(20) NOT NULL,
      |  `GRADE` int DEFAULT NULL,
      |  `OPENING_AMT` decimal(12,2) NOT NULL,
      |  `RECEIVE_AMT` decimal(12,2) NOT NULL,
      |  `PAYMENT_AMT` decimal(12,2) NOT NULL,
      |  `OUTSTANDING_AMT` decimal(12,2) NOT NULL,
      |  `PHONE_NO` varchar(17) NOT NULL,
      |  `AGENT_CODE` char(6) NOT NULL
      |) ENGINE=InnoDB;
      |""".stripMargin

}
