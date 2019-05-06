package thoughtworks

import org.apache.spark.sql.{Dataset, Row}

case class EmployeeAnalyzer(employeeDF: Dataset[Row], organizationDF: Dataset[Row]) {

  def getEmployeesWorkingInOrganization: Dataset[Row] = {
    ???
  }

}
