import sbt._
import sbt.Keys._


object ActivatorSparkBuild extends Build {

  lazy val activatorspark = Project(
    id = "Activator-Spark",
    base = file("."),
    settings = Defaults.coreDefaultSettings ++ Seq(
      // Must run the examples and tests in separate JVMs to avoid mysterious
      // scala.reflect.internal.MissingRequirementError errors. (TODO)
      fork in Test := true,
      // Must run Spark tests sequentially because they compete for port 4040!
      parallelExecution in Test := false,
      javaOptions in Test ++= Seq("-Xms1000M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")))
}
