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

libraryDependencies  ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze" % "1.3",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze-viz" % "1.3"
)
//error importing the stemming library, looks like it doesn't support scala 2.12
//which is mandatory version for spark