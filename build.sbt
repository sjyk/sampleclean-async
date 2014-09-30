import AssemblyKeys._

assemblySettings

name := "SampleClean Spark 1.0"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.0"

libraryDependencies += "com.twitter" %% "finagle-http" % "6.2.0"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.1.0"

libraryDependencies += "org.apache.hive" % "hive" % "0.14.0-SNAPSHOT"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.1.0"


libraryDependencies ++= Seq(
    "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" artifacts Artifact("javax.servlet", "jar", "jar"),
    "org.eclipse.jetty.orbit" % "javax.transaction" % "1.1.1.v201105210645" artifacts Artifact("javax.transaction", "jar", "jar"),
    "org.eclipse.jetty.orbit" % "javax.mail.glassfish" % "1.4.1.v201005082020" artifacts Artifact("javax.mail.glassfish", "jar", "jar"),
    "org.eclipse.jetty.orbit" % "javax.activation" % "1.1.0.v201105071233" artifacts Artifact("javax.activation", "jar", "jar")
    )


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Hive Contrib Repo" at "https://repository.jboss.org/"

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("javax", "servlet", xs @ _*)           => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html"   => MergeStrategy.first
  case "application.conf"                              => MergeStrategy.concat
  case "reference.conf"                                => MergeStrategy.concat
  case "log4j.properties"                              => MergeStrategy.discard
  case m if m.toLowerCase.endsWith("manifest.mf")      => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")  => MergeStrategy.discard
  case _ => MergeStrategy.first
}
}

test in assembly := {}


