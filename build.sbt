name := "SampleClean Spark 1.0"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.0.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.0.0"

libraryDependencies += "org.apache.hive" % "hive" % "0.13.1"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.1.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
