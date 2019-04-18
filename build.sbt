name := "data-transformations"
version := "0.1"
scalaVersion := "2.12.8"

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
val sparkModuleId = (name: String) => "org.apache.spark" %% s"spark-$name" % "2.4.0"
val spark = (name: String) => sparkModuleId(name) % "provided"
val spark_test = (name: String) => sparkModuleId(name) % Test classifier "tests"

libraryDependencies ++=  Seq(
  spark_test("core"),
  spark_test("sql"),
  spark_test("catalyst"),
  scalaTest,
  spark("core"),
  spark("sql"),
  spark("catalyst")
)
fork in Test := true
