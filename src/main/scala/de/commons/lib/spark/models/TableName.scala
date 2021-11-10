package de.commons.lib.spark.models

import cats.Show

final case class TableName(value: String) extends AnyVal

object TableName {
  implicit val show: Show[TableName] = _.value
}
