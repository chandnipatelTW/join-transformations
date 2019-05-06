package thoughtworks

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{Dataset, Row}

case class EmployeeAnalyzer(employeeDF: Dataset[Row], organizationDF: Dataset[Row]) {

  def getEmployeesWorkingInOrganization: Dataset[Row] = {
    employeeDF.join(broadcast(organizationDF),
      employeeDF.col("id") === organizationDF.col("employeeId"))
  }

}
