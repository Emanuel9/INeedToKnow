name := "INeed"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"

)

//libraryDependencies ++= Seq(
//
//  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
//  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
//  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"
//
//)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

//
//libraryDependencies ++= Seq(
//
//  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
//  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
//  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"
//
//)
//
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}