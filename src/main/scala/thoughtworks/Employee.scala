package thoughtworks

import org.apache.spark.sql.SparkSession

object Employee {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Analyze Employee Data Spark App").getOrCreate()

    val employeeDF = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/employee.csv")
      .cache()

    val organizationDF = spark.read
      .option("header", "true")
      .option("inferSchema", true)
      .csv("./src/main/resources/organization.csv")
      .cache()

    println("Number of employees: " + employeeDF.count())
    println("Number of organizations: " + organizationDF.count())

    val employeeAnalyzer = EmployeeAnalyzer.apply(employeeDF, organizationDF)
    val joinedDF = employeeAnalyzer.getEmployeesWorkingInOrganization

    println(joinedDF.explain())

    joinedDF.show()
  }
}


// TODO for attendees:
// 1. Check output of joinedDF.explain()
// Check this spark class to find out how JoinSelection works.
// https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala
// Especially CanBroadCast,canBuildRight,canBuildLeft methods
// Look at: https://www.coursera.org/lecture/big-data-analysis/optimizing-joins-VZmLX