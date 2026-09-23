# 🚀 Day 16 - Spark Aggregations

## 📌 Overview

Day 16 focuses on **Aggregations in Apache Spark using Scala**.

This exercise demonstrates how Spark can be used to calculate summary statistics from DataFrames using functions such as `count`, `sum`, `avg`, `min`, and `max`.

The project also demonstrates grouping data by one or multiple columns, filtering aggregated results similar to a SQL `HAVING` clause, and performing department-wise salary and hospital revenue analysis.

---

## 🎯 Objectives

The main objectives of Day 16 are:

- Understand Spark aggregation functions
- Use `count`, `sum`, `avg`, `min`, and `max`
- Perform `groupBy` operations
- Perform aggregation using multiple grouping columns
- Apply filtering after aggregation
- Understand HAVING-like operations in Spark
- Generate department-wise salary statistics
- Calculate hospital department revenue
- Generate city-wise revenue metrics

---

## 🛠️ Technologies Used

- ☕ Java 8
- 🔥 Apache Spark 3.5.3
- 🐘 Scala 2.12.18
- 📦 SBT 1.12.11
- 🖥️ Ubuntu Linux
- 💻 IntelliJ IDEA / VS Code / Terminal

---

## 📂 Project Structure

```text
Day16-Spark-Aggregations/
│
├── .gitignore
├── build.sbt
├── README.md
│
├── project/
│   └── build.properties
│
└── src/
    └── main/
        └── scala/
            └── Day16Aggregations.scala
```

---

## 📚 Aggregation Functions Covered

### 🔢 count()

Counts the number of records.

```scala
count("*")
```

Example:

```text
Employee Count = 10
```

---

### ➕ sum()

Calculates the total value of a column.

```scala
sum("salary")
```

Example:

```text
Total Salary = 727000
```

---

### 📊 avg()

Calculates the average value.

```scala
avg("salary")
```

Example:

```text
Average Salary = 72700
```

---

### ⬇️ min()

Finds the minimum value.

```scala
min("salary")
```

Example:

```text
Minimum Salary = 48000
```

---

### ⬆️ max()

Finds the maximum value.

```scala
max("salary")
```

Example:

```text
Maximum Salary = 110000
```

---

## 👥 Basic Aggregation

The project calculates overall employee salary statistics:

```scala
employees.agg(
  count("*").alias("employee_count"),
  sum("salary").alias("total_salary"),
  avg("salary").alias("average_salary"),
  min("salary").alias("minimum_salary"),
  max("salary").alias("maximum_salary")
)
```

### Result

| Metric | Value |
|---|---:|
| Employee Count | 10 |
| Total Salary | 727000 |
| Average Salary | 72700 |
| Minimum Salary | 48000 |
| Maximum Salary | 110000 |

---

## 🏢 Department-wise Salary Statistics

The project groups employees by department:

```scala
employees
  .groupBy("department")
  .agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("average_salary"),
    min("salary").alias("minimum_salary"),
    max("salary").alias("maximum_salary")
  )
```

### Department Results

| Department | Employees | Total Salary | Average Salary |
|---|---:|---:|---:|
| Finance | 3 | 255000 | 85000 |
| IT | 3 | 237000 | 79000 |
| Sales | 2 | 125000 | 62500 |
| HR | 2 | 110000 | 55000 |

---

## 🏙️ Multiple Column Grouping

Spark allows grouping using more than one column.

Example:

```scala
employees
  .groupBy("department", "city")
  .agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("average_salary")
  )
```

This produces statistics based on combinations such as:

```text
Department + City
```

For example:

```text
IT + Hyderabad
Finance + Delhi
HR + Chennai
Sales + Mumbai
```

---

## 🔎 HAVING-like Filtering

Spark DataFrames can filter aggregated results after `groupBy()` and `agg()`.

Example:

```scala
employees
  .groupBy("department")
  .agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("average_salary")
  )
  .filter(col("average_salary") > 70000)
```

This is similar to the SQL:

```sql
HAVING AVG(salary) > 70000
```

The project identifies departments whose average salary is greater than `70000`.

---

## 🏥 Hospital Revenue Analysis

The second part of the project uses hospital department data.

The dataset contains:

- Record ID
- Department
- City
- Number of patients
- Revenue per patient

---

## 💰 Total Revenue Calculation

Total revenue is calculated using:

```scala
col("patients") * col("revenue_per_patient")
```

The calculated column is added using:

```scala
val hospitalWithRevenue = hospital.withColumn(
  "total_revenue",
  col("patients") * col("revenue_per_patient")
)
```

---

## 🏥 Hospital Department Revenue Metrics

The project groups hospital records by department and calculates:

- Total patients
- Total revenue
- Average revenue per patient
- Minimum record revenue
- Maximum record revenue

```scala
hospitalWithRevenue
  .groupBy("department")
  .agg(
    sum("patients").alias("total_patients"),
    sum("total_revenue").alias("total_revenue"),
    avg("revenue_per_patient").alias("avg_revenue_per_patient"),
    min("total_revenue").alias("minimum_record_revenue"),
    max("total_revenue").alias("maximum_record_revenue")
  )
```

### Revenue Results

| Department | Total Patients | Total Revenue |
|---|---:|---:|
| Cardiology | 215 | 1034000 |
| Neurology | 145 | 857000 |
| Pediatrics | 285 | 753000 |
| Orthopedics | 200 | 745000 |
| General Medicine | 375 | 710000 |

---

## 🚨 High Revenue Departments

The project filters departments with total revenue greater than `800000`.

```scala
hospitalDepartmentMetrics
  .filter(col("total_revenue") > 800000)
```

This identifies:

```text
Cardiology
Neurology
```

---

## 🌆 City-wise Hospital Revenue

The project also calculates hospital revenue by city:

```scala
hospitalWithRevenue
  .groupBy("city")
  .agg(
    sum("patients").alias("total_patients"),
    sum("total_revenue").alias("total_revenue"),
    avg("revenue_per_patient").alias("avg_revenue_per_patient")
  )
```

### City Results

| City | Total Patients | Total Revenue |
|---|---:|---:|
| Hyderabad | 215 | 1034000 |
| Bangalore | 145 | 857000 |
| Mumbai | 285 | 753000 |
| Chennai | 200 | 745000 |
| Delhi | 375 | 710000 |

---

## 🧠 Key Learnings

Through this exercise, I learned:

- How Spark aggregation functions work
- How to use `count()`
- How to use `sum()`
- How to use `avg()`
- How to use `min()`
- How to use `max()`
- How to group data using `groupBy()`
- How to group using multiple columns
- How to filter aggregated results
- How Spark DataFrame filtering can be used like SQL `HAVING`
- How aggregation can be applied to real-world business data
- How to calculate department-wise and city-wise metrics

---

## ▶️ How to Run

### Step 1: Clone the Repository

```bash
git clone https://github.com/Harshita245-tech/Day16-Spark-Aggregations.git
```

### Step 2: Navigate to the Project

```bash
cd Day16-Spark-Aggregations
```

### Step 3: Compile

```bash
sbt compile
```

### Step 4: Run

```bash
sbt run
```

---

## ✅ Execution Result

The project executed successfully.

```text
==============================================
DAY 16 AGGREGATIONS COMPLETED
==============================================

[success] Total time: 72 s
```

---

## 📌 Project Information

**Project:** Day 16 - Spark Aggregations

**Repository:** `Day16-Spark-Aggregations`

**Language:** Scala

**Framework:** Apache Spark

**Spark Version:** 3.5.3

**Scala Version:** 2.12.18

**Build Tool:** SBT

**Status:** ✅ Completed

---

⭐ This project is part of my Apache Spark and Data Engineering training journey.
