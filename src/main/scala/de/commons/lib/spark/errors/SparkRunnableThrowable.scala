package de.commons.lib.spark.errors

final case class SparkRunnableThrowable(t: Throwable) extends Throwable {
  override def getMessage: String = s"Spark ZIO Service Exception because of reason: ${t.getMessage}"
}
