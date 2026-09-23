import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day16Aggregations {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // Spark Session
    // --------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Day 16 - Aggregations")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    println("==============================================")
    println("DAY 16 - AGGREGATIONS")
    println("==============================================")

    // --------------------------------------------------
    // 1. Employee Salary Dataset
    // --------------------------------------------------

    val employeeData = Seq(
      ("E001", "Aarav", "IT", "Hyderabad", 55000.0),
      ("E002", "Diya", "IT", "Bangalore", 72000.0),
      ("E003", "Rahul", "HR", "Hyderabad", 48000.0),
      ("E004", "Ananya", "HR", "Chennai", 62000.0),
      ("E005", "Arjun", "Finance", "Mumbai", 85000.0),
      ("E006", "Meera", "Finance", "Delhi", 92000.0),
      ("E007", "Kiran", "IT", "Hyderabad", 110000.0),
      ("E008", "Sneha", "Sales", "Mumbai", 58000.0),
      ("E009", "Vikram", "Sales", "Delhi", 67000.0),
      ("E010", "Priya", "Finance", "Hyderabad", 78000.0)
    )

    val employees = employeeData.toDF(
      "employee_id",
      "name",
      "department",
      "city",
      "salary"
    )

    println("\n===== EMPLOYEE DATA =====")

    employees.show()

    // --------------------------------------------------
    // 2. Basic Aggregations
    // --------------------------------------------------

    println("\n===== BASIC AGGREGATIONS =====")

    val basicAggregations = employees.agg(
      count("*").alias("employee_count"),
      sum("salary").alias("total_salary"),
      avg("salary").alias("average_salary"),
      min("salary").alias("minimum_salary"),
      max("salary").alias("maximum_salary")
    )

    basicAggregations.show()

    // --------------------------------------------------
    // 3. Department-wise Salary Statistics
    // --------------------------------------------------

    println("\n===== DEPARTMENT-WISE SALARY STATISTICS =====")

    val departmentStatistics = employees
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        sum("salary").alias("total_salary"),
        avg("salary").alias("average_salary"),
        min("salary").alias("minimum_salary"),
        max("salary").alias("maximum_salary")
      )
      .orderBy(desc("total_salary"))

    departmentStatistics.show()

    // --------------------------------------------------
    // 4. GroupBy with Multiple Columns
    // --------------------------------------------------

    println("\n===== DEPARTMENT + CITY AGGREGATION =====")

    val departmentCityStatistics = employees
      .groupBy("department", "city")
      .agg(
        count("*").alias("employee_count"),
        sum("salary").alias("total_salary"),
        avg("salary").alias("average_salary")
      )
      .orderBy("department", "city")

    departmentCityStatistics.show()

    // --------------------------------------------------
    // 5. HAVING-like Filtering
    // --------------------------------------------------

    println("\n===== HAVING-LIKE FILTERING =====")

    val highSalaryDepartments = employees
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        sum("salary").alias("total_salary"),
        avg("salary").alias("average_salary")
      )
      .filter(col("average_salary") > 70000)
      .orderBy(desc("average_salary"))

    highSalaryDepartments.show()

    // --------------------------------------------------
    // 6. Department-wise Salary Report
    // --------------------------------------------------

    println("\n===== DEPARTMENT SALARY REPORT =====")

    employees
      .groupBy("department")
      .agg(
        count("*").alias("employees"),
        round(avg("salary"), 2).alias("avg_salary"),
        sum("salary").alias("total_salary"),
        min("salary").alias("lowest_salary"),
        max("salary").alias("highest_salary")
      )
      .orderBy(desc("avg_salary"))
      .show()

    // --------------------------------------------------
    // 7. Hospital Department Revenue Dataset
    // --------------------------------------------------

    val hospitalData = Seq(
      ("H001", "Cardiology", "Hyderabad", 120, 4500.0),
      ("H002", "Cardiology", "Hyderabad", 95, 5200.0),
      ("H003", "Neurology", "Bangalore", 80, 6000.0),
      ("H004", "Neurology", "Bangalore", 65, 5800.0),
      ("H005", "Orthopedics", "Chennai", 110, 3500.0),
      ("H006", "Orthopedics", "Chennai", 90, 4000.0),
      ("H007", "Pediatrics", "Mumbai", 150, 2500.0),
      ("H008", "Pediatrics", "Mumbai", 135, 2800.0),
      ("H009", "General Medicine", "Delhi", 200, 1800.0),
      ("H010", "General Medicine", "Delhi", 175, 2000.0)
    )

    val hospital = hospitalData.toDF(
      "record_id",
      "department",
      "city",
      "patients",
      "revenue_per_patient"
    )

    println("\n===== HOSPITAL DATA =====")

    hospital.show()

    // --------------------------------------------------
    // 8. Calculate Total Revenue
    // --------------------------------------------------

    val hospitalWithRevenue = hospital.withColumn(
      "total_revenue",
      col("patients") * col("revenue_per_patient")
    )

    println("\n===== HOSPITAL REVENUE DATA =====")

    hospitalWithRevenue.show()

    // --------------------------------------------------
    // 9. Hospital Department Revenue Metrics
    // --------------------------------------------------

    println("\n===== HOSPITAL DEPARTMENT REVENUE METRICS =====")

    val hospitalDepartmentMetrics = hospitalWithRevenue
      .groupBy("department")
      .agg(
        sum("patients").alias("total_patients"),
        sum("total_revenue").alias("total_revenue"),
        avg("revenue_per_patient").alias("avg_revenue_per_patient"),
        min("total_revenue").alias("minimum_record_revenue"),
        max("total_revenue").alias("maximum_record_revenue")
      )
      .orderBy(desc("total_revenue"))

    hospitalDepartmentMetrics.show()

    // --------------------------------------------------
    // 10. High Revenue Departments
    // --------------------------------------------------

    println("\n===== HIGH REVENUE DEPARTMENTS =====")

    hospitalDepartmentMetrics
      .filter(col("total_revenue") > 800000)
      .show()

    // --------------------------------------------------
    // 11. Hospital City-wise Revenue
    // --------------------------------------------------

    println("\n===== CITY-WISE HOSPITAL REVENUE =====")

    hospitalWithRevenue
      .groupBy("city")
      .agg(
        sum("patients").alias("total_patients"),
        sum("total_revenue").alias("total_revenue"),
        avg("revenue_per_patient").alias("avg_revenue_per_patient")
      )
      .orderBy(desc("total_revenue"))
      .show()

    // --------------------------------------------------
    // 12. Aggregation Explanation
    // --------------------------------------------------

    println("\n===== AGGREGATION EXPLANATION =====")

    println("count() calculates the number of records.")
    println("sum() calculates the total value.")
    println("avg() calculates the average value.")
    println("min() finds the minimum value.")
    println("max() finds the maximum value.")

    println("\ngroupBy() creates groups before aggregation.")

    println(
      "Filtering after aggregation works like a SQL HAVING clause."
    )

    println(
      "In this project, aggregation is used for employee salary statistics and hospital revenue analysis."
    )

    // --------------------------------------------------
    // Completion
    // --------------------------------------------------

    println("\n==============================================")
    println("DAY 16 AGGREGATIONS COMPLETED")
    println("==============================================")

    spark.stop()
  }
}
