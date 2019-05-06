package thoughtworks

import org.apache.spark.sql.SparkSession

object Shopping {
  val spark: SparkSession = SparkSession.builder().appName("Analyze Shopping Data Spark App").getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    def customerDF = Seq(
      (1, "Alex", "M"),
      (2, "Bill", "M"),
      (3, "Steffi", "F"),
      (4, "Mary", "F"),
      (5, "Julia", "F")
    ).toDF("customerId", "name", "genderCode")

    def orderDF = Seq(
      (1, 1, 5.0),
      (2, 2, 10.0),
      (3, 2, 20.0),
      (4, 2, 30.0),
      (5, 2, 40.0),
      (6, 7, 50.0)
    ).toDF("orderId", "customerId", "amount")

    def genderDF = Seq(
      ("M", "Male"),
      ("F", "Female"),
      ("O", "Other")
    ).toDF("genderCode", "gender")

    val sellAnalyzer = SellAnalyzer.apply(customerDF, orderDF, genderDF)

    val ordersWithCustomerNameAndGender = sellAnalyzer.getOrdersWithCustomerNameAndGender.collect()
    val allUniqueCustomersWithMinimumOneOrder = sellAnalyzer.getUniqueCustomersWithMinimumOneOrder.collect()
    val allCustomersWithZeroOrMoreOrders = sellAnalyzer.getCustomersWithZeroOrMoreOrders.collect()
    val ordersWhoseCustomerIdIsNotPresentInCustomer = sellAnalyzer.getOrdersWithCustomerIdAbsentInCustomer.collect()
    val allPossibleCustomerAndGendersCombinations = sellAnalyzer.getAllPossibleCustomerAndGendersCombinations.collect()

    println("Orders with customer name and gender :")
    ordersWithCustomerNameAndGender.foreach(println)
    println("All unique customers with minimum one order :")
    allUniqueCustomersWithMinimumOneOrder.foreach(println)
    println("All customers with zero or more orders :")
    allCustomersWithZeroOrMoreOrders.foreach(println)
    println("Orders whose customerId is not present in customer table :")
    ordersWhoseCustomerIdIsNotPresentInCustomer.foreach(println)
    println("All possible customer and genders combinations :")
    allPossibleCustomerAndGendersCombinations.foreach(println)

  }

  // TODO for attendees:
  // 1. How many types of joins are possible in spark.
  // 2. How partitions help to do map-side joins.
  // 3. Importance to filter before join. What is predicate push downs why it is important and What filter conditions are not possible push down.
  // 4. What to consider when joining dataframes are loaded from a different sources (like file joins with jdbc, s3 joins with hdfs).
  // 5. What do you do when we have highly skew data while joining. For Skew in data please look below links:
  // https://medium.com/simpl-under-the-hood/spark-protip-joining-on-skewed-dataframes-7bfa610be704
  // https://bigdatacraziness.wordpress.com/2018/01/05/oh-my-god-is-my-data-skewed/
}
