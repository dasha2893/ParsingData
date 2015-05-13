name := "scalaTestProject"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.2.1",
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4"
)