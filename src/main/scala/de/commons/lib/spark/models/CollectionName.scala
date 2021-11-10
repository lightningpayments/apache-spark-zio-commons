package de.commons.lib.spark.models

import cats.Show

final case class CollectionName(value: String) extends AnyVal

object CollectionName {
  implicit val show: Show[CollectionName] = _.value
}
