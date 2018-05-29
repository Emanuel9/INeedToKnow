name := "INeed"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3",
  "com.koddi" %% "geocoder" % "1.1.0",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.1",  //Dispatch is a wrapper over async http library from apache
  "org.json4s" %% "json4s-native" % "3.2.9", //json4s support for working with json serialization/deserialization
  "org.json4s" %% "json4s-jackson" % "3.2.9"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

