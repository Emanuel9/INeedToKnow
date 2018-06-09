name := Settings.ProjectName

version := Settings.ProjectVersion

scalaVersion := Settings.ScalaVersion

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.11" % Settings.SparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % Settings.SparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % Settings.SparkCassandraConnectionVersion,
  "com.koddi" %% "geocoder" % "1.1.0",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.1",
  "org.json4s" %% "json4s-native" % "3.2.9",
  "org.json4s" %% "json4s-jackson" % "3.2.9",
  "com.google.code.gson" % "gson" % "2.8.0",
  "net.ruippeixotog" %% "scala-scraper" % "2.1.0",
  "com.typesafe" % "config" % "1.3.1",
  "com.typesafe.akka" %% "akka-http" % "10.0.5",
  "org.json4s" %% "json4s-native" % "{latestVersion}",
  "org.json4s" %% "json4s-jackson" % "{latestVersion}"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}