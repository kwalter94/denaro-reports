name := "DenaroETL"
version := "1.0"
scalaVersion := "2.12.15"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.apache.hadoop" % "hadoop-client-api" % "3.3.4",
  "org.xerial" % "sqlite-jdbc" % "3.14.2",
  "com.lihaoyi" %% "ujson" % "0.9.6"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
