package de.commons.lib.spark.util.errors

final case class ValidateReadsError(override val getMessage: String) extends Throwable
