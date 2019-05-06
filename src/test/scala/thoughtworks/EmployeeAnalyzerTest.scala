package thoughtworks

import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

class EmployeeAnalyzerTest extends SparkTest {

  test("Joining one large with one medium Data Sets") {

    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()

    val employeeDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("employee.csv"))

    val organizationDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("organization.csv"))

    val employeeAnalyzer = EmployeeAnalyzer.apply(employeeDF, organizationDF)

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> SizeInMBs(3L).toBytes.toString) {
      val joinedDF = employeeAnalyzer.getEmployeesWorkingInOrganization
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
    }
  }

  // TODO for attendees:
  // 1. Check output of joinedDF.explain()
  // Check this spark class to find out how JoinSelection works.
  // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala
  // Especially CanBroadCast,canBuildRight,canBuildLeft methods
  // Look at: https://www.coursera.org/lecture/big-data-analysis/optimizing-joins-VZmLX
}
