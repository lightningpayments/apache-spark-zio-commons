package de.commons.lib.spark.models

import cats.Show

final case class SqlQuery(value: String) extends AnyVal

object SqlQuery {
  implicit val show: Show[SqlQuery] = _.value
}
