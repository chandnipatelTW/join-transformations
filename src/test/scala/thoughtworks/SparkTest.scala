package thoughtworks

import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{DataFrame, QueryTest}

import scala.reflect.ClassTag

trait SparkTest extends QueryTest with SharedSQLContext {

  override protected def initializeSession(): Unit = {
    super.initializeSession()
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.ERROR.toString)
  }

  object Resource {
    def pathOf[T: ClassTag](resourceRelativePath: String): String = {
      implicitly[ClassTag[T]].runtimeClass
        .getResource(s"/$resourceRelativePath").getPath
    }
  }

  case class CSV(private val options: Map[String,String] = Map.empty) {
    def withHeaders = CSV(options + ("header" -> "true"))
    def read(path: String): DataFrame = spark.read.options(options).csv(path)
  }

  case class SizeInMBs(size: Long) {
    def toBytes = SizeInBytes(size * 1024 * 1024)

    override def toString: String = s"$size"

    def prettyPrint(): Unit = println(s"$size MBs")
  }

  case class SizeInBytes(size: Long) {
    def toMBs = SizeInMBs(size / 1024 / 1024)

    override def toString: String = s"$size"

    def prettyPrint(): Unit = println(s"$size Bytes")

  }

  implicit def JoinTypeToString(jt: JoinType) = jt.toString

}
