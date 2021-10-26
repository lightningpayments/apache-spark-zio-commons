# README

## minimal requirements

### application.conf

```
spark {
  appName = "SparkApp"
  master = "local[*]"
  config {}
  db {
    config {
      "driver": "com.mysql.cj.jdbc.Driver"
      "url": "jdbc:mysql://localhost:3306/sparkdb"
      "user": "<user>"
      "password": "<password>"
    }
  }
}
```

### minimal session load

```scala
implicit val configuration: Configuration = Configuration.load("path/to/application.conf")
val loader: SparkSessionLoader = new SparkSessionLoader(configuration)
```

### session load with SparkDataFrameWriter and/or SparkDBDataFrameReader

```scala
val configuration: Configuration = Configuration.load("path/to/application.conf")
val dbConf = configuration.get[Map[String, String]]("spark.db.config")
val url = configuration.get[String]("spark.db.config.url")
val properties = {
  val props = new Properties()
  props.putAll(dbConf.asJava)
  props
}
val loader: SparkSessionLoader = new SparkSessionLoader(configuration)
```

```scala
// url and props needed for e.g.
trait SparkDBDataFrameReader {
  def reader(
    sparkSession: SparkSession)(
    url: String,
    properties: Properties)(
    query: SqlQuery
  ): DataFrame = sparkSession.read.jdbc(url, query.value, properties)
}

object SparkDBDataFrameReader extends SparkDBDataFrameReader {
  type DataFrameReader = SqlQuery => DataFrame
}

// later
def getCustomers: ZIO[SparkEnvironment with SparkDBDataFrameReader, Throwable, Dataset[Customer]] = {
  for {
    env <- ZIO.environment[SparkEnvironment with SparkDBDataFrameReader]
    _   <- env.loggerM.map(_.debug("select all customers"))
    ds  <- readerM.map(Customer.select)
  } yield ds
}

private val readerM: ZIO[SparkEnvironment with SparkDBDataFrameReader, Throwable, SqlQuery => DataFrame] =
  for {
    env    <- ZIO.environment[SparkEnvironment with SparkDBDataFrameReader]
    spark  <- env.sparkM
    reader  = env.reader(spark)(url, properties)(_)
  } yield reader
```
