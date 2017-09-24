name := "spark-data-mining"

version := "1.0"

val scalaV = "2.11.11"

scalaVersion := scalaV

lazy val commonSettings = Seq(
  scalaVersion := scalaV
)

lazy val dataGenerator = (project in file("data-generator"))
  .settings(commonSettings)

lazy val sparkImpl = (project in file("spark-impl"))
  .settings(commonSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.1.1",

      "org.scalatest" %% "scalatest" % "3.0.1" % Test,
      "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test
    ),
    scalacOptions ++= Seq("-optimize", "-Yinline-warnings")
  ))
