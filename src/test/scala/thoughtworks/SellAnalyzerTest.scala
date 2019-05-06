package thoughtworks

class SellAnalyzerTest extends SparkTest {

  import testImplicits._

  object TestData {
    def customers = Seq(
      (1, "Alex", "M"),
      (2, "Bill", "M"),
      (3, "Steffi", "F"),
      (4, "Mary", "F"),
      (5, "Julia", "F")
    ).toDF("customerId", "name", "genderCode")

    def orders = Seq(
      (1, 1, 5.0),
      (2, 2, 10.0),
      (3, 2, 20.0),
      (4, 2, 30.0),
      (5, 2, 40.0),
      (6, 7, 50.0)
    ).toDF("orderId", "customerId", "amount")

    def genders = Seq(
      ("M", "Male"),
      ("F", "Female"),
      ("O", "Other")
    ).toDF("genderCode", "gender")
  }


  test("orders with customer name and gender") {
    val sellAnalyzer = SellAnalyzer.apply(TestData.customers, TestData.orders, TestData.genders)

    val actualDF = sellAnalyzer.getOrdersWithCustomerNameAndGender

    val expectedDF = Seq(
      ("M", 1, "Alex", 1, 5.0, "Male"),
      ("M", 2, "Bill", 2, 10.0, "Male"),
      ("M", 2, "Bill", 3, 20.0, "Male"),
      ("M", 2, "Bill", 4, 30.0, "Male"),
      ("M", 2, "Bill", 5, 40.0, "Male")
    ).toDF("genderCode", "customerId", "name", "orderId", "amount", "gender")

    checkAnswer(actualDF, expectedDF)
  }

  test("all unique customers with minimum one order") {
    val sellAnalyzer = SellAnalyzer.apply(TestData.customers, TestData.orders, TestData.genders)

    val actualDF = sellAnalyzer.getUniqueCustomersWithMinimumOneOrder

    val expectedDF = Seq(
      (1, "Alex", "M"),
      (2, "Bill", "M")
    ).toDF("customerId", "name", "genderCode")

    checkAnswer(actualDF, expectedDF)
  }

  test("all customers with zero or more orders") {
    val sellAnalyzer = SellAnalyzer.apply(TestData.customers, TestData.orders, TestData.genders)

    val actualDF = sellAnalyzer.getCustomersWithZeroOrMoreOrders

    val expectedDF = Seq(
      (1, "Alex", "M", Some(1), Some(5.0)),
      (2, "Bill", "M", Some(2), Some(10.0)),
      (2, "Bill", "M", Some(3), Some(20.0)),
      (2, "Bill", "M", Some(4), Some(30.0)),
      (2, "Bill", "M", Some(5), Some(40.0)),
      (3, "Steffi", "F", None, None),
      (4, "Mary", "F", None, None),
      (5, "Julia", "F", None, None)
    ).toDF("customerId", "name", "genderCode", "orderId", "amount")

    checkAnswer(actualDF, expectedDF)
  }

  test("orders whose customerId is not present in customer table") {
    val sellAnalyzer = SellAnalyzer.apply(TestData.customers, TestData.orders, TestData.genders)

    val actualDF = sellAnalyzer.getOrdersWithCustomerIdAbsentInCustomer

    val expectedDF = Seq(
      (1, "Alex", "M", 1, 5.0),
      (2, "Bill", "M", 2, 10.0),
      (2, "Bill", "M", 3, 20.0),
      (2, "Bill", "M", 4, 30.0),
      (2, "Bill", "M", 5, 40.0),
      (7, null, null, 6, 50.0)
    ).toDF("customerId", "name", "genderCode", "orderId", "amount")

    checkAnswer(actualDF, expectedDF)
  }

  test("all possible customer and genders combinations") {
    val sellAnalyzer = SellAnalyzer.apply(TestData.customers, TestData.orders, TestData.genders)

    val actualDF = sellAnalyzer.getAllPossibleCustomerAndGendersCombinations

    val expectedDF = Seq(
      (1,"Alex","Female"),
      (1,"Alex","Male"),
      (1,"Alex","Other"),
      (2,"Bill","Female"),
      (2,"Bill","Male"),
      (2,"Bill","Other"),
      (3,"Steffi","Female" ),
      (3,"Steffi","Male"),
      (3,"Steffi","Other"),
      (4,"Mary","Female"),
      (4,"Mary","Male"),
      (4,"Mary","Other"),
      (5,"Julia","Female"),
      (5,"Julia","Male"),
      (5,"Julia","Other")).toDF("customerId", "name", "gender")

    checkAnswer(actualDF, expectedDF)
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
