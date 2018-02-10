name := "spark-data-mining"

version := "1.0"

val scalaV = "2.11.11"

scalaVersion := scalaV

lazy val commonSettings = Seq(
  scalaVersion := scalaV,
  scalacOptions ++= Seq(
    "-optimize",
    "-Yinline-warnings",
    "-Xelide-below", "3000"))

lazy val dataGenerator = (project in file("data-generator"))
  .settings(commonSettings)
  .dependsOn(sparkImpl)

lazy val sparkImpl = (project in file("spark-impl"))
  .settings(commonSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",

      "org.clapper" %% "grizzled-slf4j" % "1.3.1",

      "org.scalatest" %% "scalatest" % "3.0.1" % Test,
      "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test),
    parallelExecution in Test := false,
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  ))

lazy val sparkImplRunner = (project in file("spark-impl-runner"))
  .dependsOn(sparkImpl)
  .settings(commonSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",

      "org.scalatest" %% "scalatest" % "3.0.1" % Test
    ),
    parallelExecution in Test := false,
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  ))

lazy val sparkImplPerfTest = (project in file("spark-impl-perf-test"))
  .dependsOn(sparkImpl)
  .settings(commonSettings ++ Seq(
    resolvers += "Sonatype OSS Snapshots" at
      "https://oss.sonatype.org/content/repositories/snapshots",
    libraryDependencies ++= Seq(
      "com.storm-enroute" %% "scalameter" % "0.8.2"),
    parallelExecution in Test := false,
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    logBuffered := false))

(assembly in sparkImplRunner) := ((assembly in sparkImplRunner) dependsOn (assembly in sparkImpl)).value
