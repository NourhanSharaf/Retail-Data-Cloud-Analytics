// Databricks notebook source
// MAGIC %md 
// MAGIC Reading the by-day data as static at first

// COMMAND ----------

/** Reading the static version of the dataset as a DataFrame **/
val retailData_static = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/FileStore/tables/by-day")

// val static = spark.read.csv("/FileStore/tables/retaildata/")
val dataSchema = retailData_static.schema

// COMMAND ----------

// retailData_static.show()
val df1 = retailData_static.toDF()
display(df1)

// COMMAND ----------

  df1.count()

// COMMAND ----------

// MAGIC %md 
// MAGIC Reading the by-day data as stream

// COMMAND ----------

/* Setting up a schema that reads one file per trigger */
val retailData_streaming = spark
                  .readStream.schema(dataSchema)
                  .option("maxFilesPerTrigger", 20)
                  .csv("/FileStore/tables/by-day")
 

// COMMAND ----------

/* Setting the shuffle partition limit allows you to limit the level of paritioning.*/
spark.conf.set("spark.sql.shuffle.partitions", 5)

// COMMAND ----------

  /* To use Spark SQL on a DataFrame or to perform certain complex filtering, we need to create a temp view from the DataFrame */
retailData_streaming.createOrReplaceTempView("retailData_streaming")

// COMMAND ----------

import org.apache.spark.sql.functions.{sum , count, col , lit , current_timestamp}

// COMMAND ----------

/* Querying for the active streams under our Spark session */
spark.streams.active

// COMMAND ----------

// MAGIC %md
// MAGIC **b) Load the retail data as a stream, at 20 files per trigger. For each batch pulled, capture 
// MAGIC the customer stock aggregates – total stocks, total value.**

// COMMAND ----------

/* For each batch, we group by the gt field and add to activity counts */
 val customer_stock_aggregate =retailData_streaming
                               .groupBy("CustomerID")
                               .agg(sum("Quantity").alias("total stock"),sum(col ("Quantity")* col ("UnitPrice")).alias("total value"))

// COMMAND ----------

/* 
activity count is now our output stream. We will query the aggregate count field into out write stream.

By using the complete output mode, we are telling Spark to rewrite all the keys with their counts after each trigger.
Complete mode does not drop old aggregation state and preserves all data in the Result Table.
*/
val activityQuery = customer_stock_aggregate.writeStream.queryName("activity_counts")
  .format("memory").outputMode("complete")
  .start()


// COMMAND ----------

/* 
  activity_counts is a table so we can use sparkSql to query it.
  So we can re-run the query every second to see the progression.
 */
for( i <- 1 to 20 ) {
    spark.sql("SELECT * FROM activity_counts").show()
    Thread.sleep(1000)
}


// COMMAND ----------

// MAGIC %md
// MAGIC **c)  For each batch of the input stream, create a new stream that populates another 
// MAGIC dataframe or dataset with progress for each loaded set of data. This data set should 
// MAGIC have the columns – TriggerTime (Date/Time), Records Imported, Sale value (Total 
// MAGIC value of transactions)**

// COMMAND ----------

// import spark.sqlContext.implicits._

// COMMAND ----------

val new_stream = retailData_streaming
               .withColumn("TriggerTime",current_timestamp().as("TriggerTime"))
               .withColumn("total value", col ("Quantity")* col ("UnitPrice"))
               .withColumn("Records Imported", lit(1))
               .groupBy("TriggerTime")
               .agg(count("Records Imported").alias("Records Imported"),sum("total value").alias("total value"))


// COMMAND ----------

val new_streamQuery = new_stream.writeStream.queryName("newstream")
  .format("memory").outputMode("complete")
  .start()

// COMMAND ----------

for( i <- 1 to 20 ) {
    spark.sql("SELECT * FROM newstream").show()
    Thread.sleep(1000)
}

// COMMAND ----------

// MAGIC %md
// MAGIC **d) Use the dataset from step (c) to plot a line graph of the import process – showing two 
// MAGIC timelines – records imported and sale values.**

// COMMAND ----------

display(new_stream)
