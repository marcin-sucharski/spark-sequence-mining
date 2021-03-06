import pl.project13.scala.sbt.JmhPlugin

val scalaV = "2.11.11"

lazy val commonSettings = Seq(
  scalaVersion := scalaV,
  scalacOptions ++= Seq(
    "-optimize",
    "-Yinline-warnings"
  ),
  scalacOptions in assembly ++= Seq(
    "-Xelide-below", "3000"
  ),
  testOptions in ThisBuild ++= Seq(
    Tests.Setup(_ => sys.props("testing") = "true"),
    Tests.Cleanup(_ => sys.props.remove("testing"))
  ),
  version := (version in ThisBuild).value,
  organization := "one.off_by"
)

def testCoverage = Command.command("testCoverage") { state =>
  state.copy(
    remainingCommands = List(
      Exec("clean", None),
      Exec("coverageOn", None),
      Exec("test", None),
      Exec("coverageReport", None),
      Exec("coverageOff", None)
    ) ++ state.remainingCommands
  )
}

lazy val root = (project in file("."))
  .settings(commonSettings ++ Seq(
    name := "spark-data-mining",
    assembly := (assembly in sparkImplRunner).value,
    test in Test := ((test in sparkImplRunner in Test) dependsOn (test in sparkImpl in Test)).value,
    fork in Test in ThisBuild := coverageEnabled.value,
    javaOptions in Test in ThisBuild ++= {
      if (coverageEnabled.value) Seq(
        "-Dlog4j.configuration=log4j.coverage.properties",
        "-Dtesting=true"
      )
      else Nil
    },
    commands ++= Seq(testCoverage)
  ))
  .aggregate(sparkImpl, sparkImplRunner, dataGenerator, sparkImplPerfTest)

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
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case _                           => MergeStrategy.first
    },
    assemblyJarName := s"spark-gsp-${version.value}.jar"
  ))

lazy val sparkImplRunner = (project in file("spark-impl-runner"))
  .dependsOn(sparkImpl)
  .settings(commonSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
      "com.github.tototoshi" %% "scala-csv" % "1.3.5",
      "io.spray" %% "spray-json" % "1.3.3",

      "org.scalatest" %% "scalatest" % "3.0.1" % Test
    ),
    parallelExecution in Test := false,
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case _                           => MergeStrategy.first
    },
    assemblyJarName := s"spark-gsp-runner-${version.value}.jar",
    assembly := (assembly dependsOn (assembly in sparkImpl)).value
  ))

lazy val sparkImplPerfTest = (project in file("spark-impl-perf-test"))
  .dependsOn(sparkImpl)
  .settings(commonSettings ++ Seq(
    parallelExecution in Test := false,
    logBuffered := false))
  .enablePlugins(JmhPlugin)
