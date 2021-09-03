name := "LatentSemanticIndexing_IRProject"

version := "0.1"

scalaVersion := "2.12.14"
//scalaVersion := "2.10"
val sparkVersion = "3.1.2"
idePackagePrefix := Some("org.ir.project")

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

libraryDependencies ++= sparkDependencies
//error importing the stemming library, looks like it doesn't support scala 2.12
//which is mandatory version for spark
//libraryDependencies += "com.github.master" %% "spark-stemming" % "0.2.1"