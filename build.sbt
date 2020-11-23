name := "WordCountTDD"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.8.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)