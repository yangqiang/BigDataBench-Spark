name := "BigDataBench-Spark"

version := "0.8.0-incubating"

scalaVersion := "2.9.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.8.0-incubating"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "0.8.0-incubating"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.0.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
