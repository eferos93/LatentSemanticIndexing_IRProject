name := "LatentSemanticIndexing_IRProject"

version := "0.1"

scalaVersion := "2.12.14"
val sparkVersion = "3.1.2"
idePackagePrefix := Some("org.ir.project")

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)