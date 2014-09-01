name := "SampleClean Spark 1.0"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.0.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.0.0"

libraryDependencies += "org.apache.hive" % "hive" % "0.13.1"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.1.0"


libraryDependencies ++= Seq(
    "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" artifacts Artifact("javax.servlet", "jar", "jar"),
    "org.eclipse.jetty.orbit" % "javax.transaction" % "1.1.1.v201105210645" artifacts Artifact("javax.transaction", "jar", "jar"),
    "org.eclipse.jetty.orbit" % "javax.mail.glassfish" % "1.4.1.v201005082020" artifacts Artifact("javax.mail.glassfish", "jar", "jar"),
    "org.eclipse.jetty.orbit" % "javax.activation" % "1.1.0.v201105071233" artifacts Artifact("javax.activation", "jar", "jar")
    )


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

