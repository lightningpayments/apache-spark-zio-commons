package de.commons.lib.spark.testapps.sql101.app

import de.commons.lib.spark.environments.SparkR.SparkEnvironment
import de.commons.lib.spark.environments.io.{SparkDBDataFrameReader, SparkDataFrameWriter}
import de.commons.lib.spark.runnable.SparkIORunnable
import de.commons.lib.spark.testapps.sql101.app.logic.services.DbService
import zio.{ExitCode, URIO, ZIO}

/**
 * NOTICE: check db connection before you run this app!!!
 *
 * ### Local MySql DB Deployment ###
 *
 * docker run --name budni_dev_local_mysql -p 3306:3306 \
 * -e MYSQL_ROOT_PASSWORD=password -d mysql --default-authentication-plugin=mysql_native_password
 * docker exec -it <container-id> bash -l
 *
 * 1 pen & Edit /etc/my.cnf or /etc/mysql/my.cnf, depending on your distro.
 * 2 Add skip-grant-tables under [mysqld]
 * 3 Restart Mysql
 * 4 You should be able to login to mysql now using the below command mysql -u root -p
 * 5 Run mysql> flush privileges;
 * 6 Set new password by ALTER USER 'root' IDENTIFIED BY 'NewPassword';
 * 7 Go back to /etc/my.cnf and remove/comment skip-grant-tables
 * 8 Restart Mysql
 * 9 Now you will be able to login with the new password mysql -u root -p
 *
 * https://medium.com/tech-learn-share/docker-mysql-access-denied-for-user-172-17-0-1-using-password-yes-c5eadad582d3
 *
 * #################################
 */
private[testapps] object SimpleDbApp extends zio.App with AppConfig {

  private val env = new SparkEnvironment(configuration, logger) with SparkDBDataFrameReader with SparkDataFrameWriter

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val io: ZIO[SparkEnvironment with SparkDBDataFrameReader with SparkDataFrameWriter, Throwable, Unit] = for {
      _ <- ZIO.environment[SparkEnvironment with SparkDBDataFrameReader with SparkDataFrameWriter]
      dbService = new DbService(url, properties)

      _ <- dbService.getAgents.map(_.show())
      _ <- dbService.getCustomers.map(_.show())
      _ <- dbService.getOrders.map(_.show())
      _ <- dbService.getAgentsStatistics.map(_.show())
    } yield ()

    SparkIORunnable[SparkEnvironment, SparkDBDataFrameReader with SparkDataFrameWriter, Unit](io)
      .run
      .provide(env)
      .exitCode
  }

}
