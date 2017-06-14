name := "spark-data-mining"

version := "1.0"

scalaVersion := "2.11.11"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.11"
)

lazy val dataGenerator = (project in file("data-generator"))
  .settings(commonSettings)

lazy val sparkImpl = (project in file("spark-impl"))
  .settings(commonSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.1.1",

      "org.scalatest" %% "scalatest" % "3.0.1" % "test"
    )
  ))
