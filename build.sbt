
// -------------------------------------------------------------------------------------------------
// Package configuration
// -------------------------------------------------------------------------------------------------
name := "Commons-Spark"
organization := "de.commons"
organizationHomepage := None
organizationName := "Commons"
version := "2.0.2"

// -------------------------------------------------------------------------------------------------
// Application
// -------------------------------------------------------------------------------------------------
addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.0" cross CrossVersion.full)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// -------------------------------------------------------------------------------------------------
// Scala compiler settings
// -------------------------------------------------------------------------------------------------
fork in Test := true // @see https://github.com/sbt/sbt/issues/3022
scalacOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties"
)
outputStrategy := Some(StdoutOutput)
scalacOptions in Compile ++= Seq("-deprecation", "-explaintypes", "-feature", "-unchecked")
Compile / scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) if n == 12 => Seq("-Ypartial-unification")
    case _ => Seq.empty
  }
}
crossScalaVersions := Seq("2.12.10")
scalaVersion := crossScalaVersions.value.head
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oSD")
ThisBuild / scalacOptions += "-P:kind-projector:underscore-placeholders"
// -------------------------------------------------------------------------------------------------
// Publisher
// -------------------------------------------------------------------------------------------------
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

// -------------------------------------------------------------------------------------------------
// Library dependencies
// -------------------------------------------------------------------------------------------------
val sparkVers = "3.1.2"
lazy val log4j = "log4j" % "log4j" % "1.2.17"
lazy val slf4j = "org.slf4j" % "slf4j-api" % "1.7.16"
lazy val slf4jLog4j = "org.slf4j" % "slf4j-log4j12" % "1.7.16"

libraryDependencies ++= Seq(
  "com.typesafe.play"      %% "play-jdbc"               % "2.8.2",
  "org.typelevel"          %% "cats-core"               % "2.6.1",
  "org.typelevel"          %% "cats-effect"             % "2.5.1",
  "dev.zio"                %% "zio-interop-cats"        % "2.5.1.0",
  "dev.zio"                %% "zio"                     % "1.0.12",
  "org.apache.spark"       %% "spark-core"              % sparkVers % "compile" exclude("org.slf4j", "slf4j-log4j12"),
  // https://stackoverflow.com/questions/57149420/run-spark-locally-with-intellij
  "org.apache.spark"       %% "spark-sql"               % sparkVers % "compile" exclude("org.slf4j", "slf4j-log4j12"),
  "mysql"                   % "mysql-connector-java"    % "8.0.21",
  "org.postgresql"          % "postgresql"              % "42.2.23",
  "com.h2database"          % "h2"                      % "1.4.200" % "test",
  "org.mockito"            %% "mockito-scala-scalatest" % "1.14.8"  % "test",
  "org.mockito"             % "mockito-inline"          % "3.3.3"   % "test",
  "org.scalatestplus.play" %% "scalatestplus-play"      % "5.1.0"   % "test",
  "org.scalatestplus"      %% "scalacheck-1-14"         % "3.2.0.0" % "test",
  "org.apache.archiva"      % "archiva"                 % "2.2.5" pomOnly()
)
libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-jdk14")) }
dependencyOverrides ++= Seq(log4j, slf4j, slf4jLog4j)
// -------------------------------------------------------------------------------------------------
// Scapegoat Configuration (static code analysis)
// -------------------------------------------------------------------------------------------------
scapegoatConsoleOutput := true
scapegoatIgnoredFiles := Seq.empty
scapegoatVersion in ThisBuild := "1.4.5"
scapegoatDisabledInspections := Seq("VariableShadowing")

// -------------------------------------------------------------------------------------------------
// Scoverage Configuration (code coverage)
// -------------------------------------------------------------------------------------------------
coverageFailOnMinimum := true
coverageHighlighting := true
coverageMinimum := 100
coverageExcludedPackages := """<empty>;..*Module.*;"""

// -------------------------------------------------------------------------------------------------
// Scalastyle Configuration (check style)
// -------------------------------------------------------------------------------------------------
scalastyleFailOnError := true
