name := "BigDataBench-Spark"

version := "0.9.2"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.2"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "0.9.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.0.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
