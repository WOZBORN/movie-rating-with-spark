name := "movie-ratings-analysis"

version := "1.0"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.3",
  "org.apache.spark" %% "spark-sql" % "3.2.3"
)