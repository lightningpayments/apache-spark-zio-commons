package de.commons.lib.spark.models

import cats.Show

final case class DbName(value: String) extends AnyVal

object DbName {
  implicit val show: Show[DbName] = _.value
}
