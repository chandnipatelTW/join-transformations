package main

import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

class BroadCastJoinTest extends SparkTest {
  import testImplicits._

  test("Joining Two smaller Data Sets") {
    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()
    val df2 = spark.range(100).as("b")
    val df1 = spark.range(100).as("a")
    val joinedDF = df1.join(df2).where($"a.id" === $"b.id")
    assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 0)
  }

  test("Joining one large with another large Data Set") {

    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()

    val peopleDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people.csv"))

    val peopleDF2 = spark.read.option("header", "true")
      .csv(Resource.pathOf("people.csv"))

      val joinedDF = peopleDF.join(peopleDF2, Seq("firstName"), Inner)
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
  }

  test("Joining one large with one medium Data Sets") {

    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()

    val peopleDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people.csv"))

    val peopleSmallDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people_small.csv"))

    println(peopleDF.queryExecution.logical.stats.sizeInBytes)
    println(peopleSmallDF.queryExecution.logical.stats.sizeInBytes)
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> SizeInMBs(3L).toBytes.toString) {
      val joinedDF = peopleDF.join(peopleSmallDF, Seq("firstName"), Inner)
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
    }
  }

  test("Joining Two smaller Data Sets with broadcast threshold off") {
    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df2 = spark.range(100).as("b")
      val df1 = spark.range(100).as("a")
      val joinedDF = df1.join(df2).where($"a.id" === $"b.id")
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
      // Please don't change below assert to fix test
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: SortMergeJoinExec => p }.size === 0)
    }
  }

  test("Cross Joining Two Data Sets") {
    val peopleDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people.csv"))

    val peopleSmallDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people_small.csv"))

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> SizeInMBs(3L).toBytes.toString) {
      val joinedDF = peopleDF.crossJoin(peopleSmallDF)
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
    }
  }

  // TODO for attendees:
  // 1. Check output of joinedDF.explain()
  // Check this spark class to find out how JoinSelection works.
  // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala
  // Especially CanBroadCast,canBuildRight,canBuildLeft methods
  // Look at: https://www.coursera.org/lecture/big-data-analysis/optimizing-joins-VZmLX
}
