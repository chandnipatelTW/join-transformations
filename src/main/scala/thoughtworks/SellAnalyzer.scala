package thoughtworks

import org.apache.spark.sql.{Dataset, Row}

case class SellAnalyzer(customerDF: Dataset[Row],
                   orderDF: Dataset[Row],
                   genderDF: Dataset[Row]) {

  def getOrdersWithCustomerNameAndGender: Dataset[Row] = {
    ???
  }

  def getUniqueCustomersWithMinimumOneOrder:Dataset[Row] = {
    ???
  }

  def getCustomersWithZeroOrMoreOrders: Dataset[Row] = {
    ???
  }

  def getOrdersWithCustomerIdAbsentInCustomer: Dataset[Row] = {
    ???
  }

  def getAllPossibleCustomerAndGendersCombinations: Dataset[Row] = {
    ???
  }
}
