// Databricks notebook source
// MAGIC %md
// MAGIC start a spark session

// COMMAND ----------

/* SparkSession associated with this workbook */
spark

// COMMAND ----------

// MAGIC %md
// MAGIC listing all the file in FileStore/tables directory

// COMMAND ----------

/* Display the list of all uploaded files in your filestore */
display(dbutils.fs.ls("/FileStore/tables"))

// COMMAND ----------

// MAGIC %md
// MAGIC Reading all the csv files in the by-day folder

// COMMAND ----------

val retailData = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/FileStore/tables/by-day/*.csv")

// COMMAND ----------

// /* count the number of records 
retailData.count

// COMMAND ----------

// MAGIC %md
// MAGIC Let's change the retailData to a dataframe to  explore it

// COMMAND ----------

val df1 = retailData.toDF()
display(df1)

// COMMAND ----------

  /*  we need to creating a temp view from the DataFrame to use Spark SQL on a DataFrame or to perform certain complex filtering,*/
retailData.createOrReplaceTempView("retailData")

// COMMAND ----------

// MAGIC %md
// MAGIC **b) Output the total number of transactions across all the files and the total value of the 
// MAGIC transactions.**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT sum (Quantity) AS total__transaction,
// MAGIC        sum(UnitPrice*Quantity) AS total_Value_transactions
// MAGIC FROM retailData ;

// COMMAND ----------


// // 
// Here we calculate the total number of unique transactions  accross all the csv files and then calculate the total value of all the purchases 

// %sql
// SELECT COUNT ( DISTINCT InvoiceNo ) AS total_unique_transaction,
//        sum(UnitPrice*Quantity) AS total_Value_transactions
// FROM retailData ;

// COMMAND ----------

// Here we want to sum up same transactions together and find the corresponding total values of each


// %sql
// SELECT InvoiceNo, 
//   count(InvoiceNo) AS total_number_transaction,
//   sum(UnitPrice*Quantity) AS total_Value_transaction
// FROM retailData 
// GROUP BY InvoiceNo

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC **c) Output the 5 top-selling products.**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT StockCode, sum(Quantity) AS number_of_sold_product
// MAGIC FROM retailData 
// MAGIC GROUP BY StockCode
// MAGIC ORDER BY sum(Quantity) DESC
// MAGIC LIMIT 5

// COMMAND ----------

// MAGIC %md
// MAGIC **d) Output the 5 topmost valuable products.**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT StockCode, sum (UnitPrice*Quantity)AS SaleValue
// MAGIC FROM retailData
// MAGIC GROUP BY StockCode
// MAGIC ORDER BY sum(UnitPrice*Quantity )DESC
// MAGIC 
// MAGIC LIMIT 5

// COMMAND ----------

// MAGIC %md
// MAGIC **e) Output each country and the total value of their purchases.**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT Country,  sum (UnitPrice*Quantity) as Country_total_purchase
// MAGIC FROM retailData
// MAGIC GROUP BY Country
// MAGIC /*ORDER BY count(InvoiceNo) DESC*/

// COMMAND ----------

// MAGIC %md
// MAGIC **f)Use a graphical representation to describe the result from step (d).**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT StockCode, sum (UnitPrice*Quantity)AS SaleValue FROM retailData GROUP BY StockCode ORDER BY sum(UnitPrice*Quantity )DESC
// MAGIC LIMIT 5
