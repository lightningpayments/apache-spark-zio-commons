# README

- recommended java version: openjdk8

## install test db

```bash
# download test db image
docker pull ronakkany/spark-test-maria-db:latest
docker run --name budni_dev_local_mysql -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=password -d mysql --default-authentication-plugin=mysql_native_password
docker exec -it <container-id> bash -l

### Local MySql DB Deployment ###

# 1 pen & Edit /etc/my.cnf or /etc/mysql/my.cnf, depending on your distro.
# 2 Add skip-grant-tables under [mysqld]
# 3 Restart Mysql
# 4 You should be able to login to mysql now using the below command mysql -u root -p
# 5 Run mysql> flush privileges;
# 6 Set new password by ALTER USER 'root' IDENTIFIED BY 'NewPassword';
# 7 Go back to /etc/my.cnf and remove/comment skip-grant-tables
# 8 Restart Mysql
# 9 Now you will be able to login with the new password mysql -u root -p

# see
# https://medium.com/tech-learn-share/docker-mysql-access-denied-for-user-172-17-0-1-using-password-yes-c5eadad582d3
```

## application.conf minimal requirements

```
spark {
  appName = "SimpleDbApp"
  db {
    config {
      "driver": "com.mysql.cj.jdbc.Driver"
      "url": "jdbc:mysql://localhost:3305/sparkdb"
      "user": "ronny"
      "password": "password"
    }
  }
}
```

## Example

### SparkEnvironment example with SparkDBDataFrameReader

```scala

// url and props needed for e.g.
trait SparkDbDataFrameReader {
  def reader(
    sparkSession: SparkSession)(
    url: String,
    properties: Properties)(
    query: SqlQuery
  ): DataFrame = sparkSession.read.jdbc(url, query.value, properties)
}

object SparkDbDataFrameReader extends SparkDbDataFrameReader {
  type DataFrameReader = SqlQuery => DataFrame
}


// usage
def getCustomers: ZIO[SparkEnvironment with SparkDBDataFrameReader, Throwable, Dataset[Customer]] = {
  for {
    env <- ZIO.environment[SparkEnvironment with SparkDBDataFrameReader]
    _   <- env.loggerM.map(_.debug("select all customers"))
    ds  <- readerM.map(Customer.select) // Customer class type as a table reference
  } yield ds
}

private val readerM: ZIO[SparkEnvironment with SparkDBDataFrameReader, Throwable, SqlQuery => DataFrame] =
  for {
    env    <- ZIO.environment[SparkEnvironment with SparkDBDataFrameReader]
    spark  <- env.sparkM
    reader  = env.reader(spark)(url, properties)(_)
  } yield reader
```
