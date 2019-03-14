// Databricks notebook source
/*** NOTES ****/
/*
We need 11 groups (for the pivot)
*/

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

val spark = SparkSession.builder().getOrCreate()

// COMMAND ----------

// Time function
def time[R](text:String, block: => R): R = {
    //based on  http://biercoff.com/easily-measuring-code-execution-time-in-scala/
    println("_____________________________________________________")
    println("** " + text + " **")
    println("Begin Timing: " + System.currentTimeMillis() + "ms")
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    val tdiff =  (t1 - t0)
    println("End Timing: " + System.currentTimeMillis() + "ms")
    println("Elapsed time: " + tdiff + "ms")
    /*println("_____________________________________________________")*/
    var out  = {text+ "," + tdiff+"\r\n"}
    var filename = "/dbfs/mnt/blob/time_log_PySpark.csv"
    scala.tools.nsc.io.File(filename).appendAll(out)
    return(result)
  }

// COMMAND ----------

// Testing Time
scala.tools.nsc.io.File("time_log.csv").delete()
println("testing Time 1000ms")
time("test 1sec",{Thread.sleep(1000)})

// COMMAND ----------

var file_loc = "/dbfs"
var file_prefix = "/mnt/blob/"
var file_name = "test_dataset1"

val df = spark.read.option("header","true").option("inferSchema","true").csv(file_prefix + file_name + ".csv")
val df_100 = spark.read.option("header","true").option("inferSchema","true").csv(file_prefix + file_name + "_100.csv")
val df_1000 = spark.read.option("header","true").option("inferSchema","true").csv(file_prefix + file_name + "_1000.csv")
val df_10000 = spark.read.option("header","true").option("inferSchema","true").csv(file_prefix + file_name + "_10000.csv")
val df_join = df.limit(df.count().toInt/3) /*df2 is needed for join operations, we use half of df to allow some outer join*/
val df_join_small = df.limit(30) 



// COMMAND ----------

// DBTITLE 1,Row Operations
// filtering rows based on a condition
time("Row Operation, Filter",df.filter($"int17" > 200 && $"float17"< 200).select("int17","float17").collect() )

// Filtering rows based on regular expressions
var expr = "^troubleshot"  //var expr = "^troubleshot"
time("Row Operation, Filter Reg Ex 1",df.filter($"Group0".rlike(expr)).collect())

// rr at 3rd and 4th position and ending with w
expr = ".{2}rr.*w$"
time("Row Operation, Filter Reg Ex 2",df.filter($"Group0".rlike(expr)).collect())



// COMMAND ----------

// DBTITLE 1,Row Operation: Shift (Lag)
/* Shift (Lag) */
var w = org.apache.spark.sql.expressions.Window.orderBy("Int1")
import org.apache.spark.sql.functions.lag
time("Row Operation, Shift (Lag)",df.select("Int1").withColumn("status_lag", lag("Int1", 1, 0).over(w)).collect())


// COMMAND ----------

/** Running Sum **/
// https://stackoverflow.com/questions/46982119/how-to-calculate-cumulative-sum-in-a-pyspark-table
var w2= org.apache.spark.sql.expressions.Window.orderBy("Int1")
time("Row Operation, Running Sum",df.select("Int1").withColumn("CumSumTotal", sum("Int1") over (w2)).collect())


// COMMAND ----------

// DBTITLE 1,Row Operation: Writing new rows
time("Row Operation, Writing 100 new rows", df.union(df_100).collect())

// COMMAND ----------

time("Row Operation, Writing 1000 new rows", df.union(df_1000).collect())

// COMMAND ----------

time("Row Operation, Writing 10000 new rows", df.union(df_10000).collect())

// COMMAND ----------

// DBTITLE 1,Column Operation: Sorting
time("Column Operation, Sorting Asc 1 column",{df.sort("words0").collect()})

// COMMAND ----------

time("Column Operation, Sorting Asc 5 column",{df.sort("words0","words1","words2","words3","words4").collect()})

// COMMAND ----------

time("Column Operation, Sorting Asc 10 column",{df.sort("words0","words1","words2","words3","words4","words5","words6","words7","words8","words9").collect()})

// COMMAND ----------

time("Column Operation, Sorting Desc 1 column",{df.sort($"words0".desc).collect()})


// COMMAND ----------

time("Column Operation, Sorting Desc 5 column",{df.sort($"words0".desc,$"words1".desc,$"words2".desc,$"words3".desc,$"words4".desc).collect()})

// COMMAND ----------

time("Column Operation, Sorting Desc 10 column",{df.sort($"words0".desc,$"words1".desc,$"words2".desc,$"words3".desc,$"words4".desc,$"words5".desc,$"words6".desc,$"words7".desc,$"words8".desc,$"words9".desc).collect()})

// COMMAND ----------

// DBTITLE 1,Column Operation: Mathematical Operations on Column
time("Column Operation, Mathematical Operation on Columns",
  df.select("Int1","Float1","Int2","Float3","Float10").agg(sum("Int1"),avg("Float1"),count("Int2"),min("Float3"),max("Float10")).collect()
)

// COMMAND ----------

// DBTITLE 1,Column Operation: Join
time("Column Operation, Inner Join 1 Column", df.join(df_join_small,Seq("group0"),"inner").collect())  

// COMMAND ----------

val df_join = df.limit(df.count().toInt/2) 
time("Column Operation, Inner Join 5 Column", df.join(df_join,Seq("group0","group1","group2","group3","group4"),"inner").collect())



// COMMAND ----------

time("Column Operation, Inner Join 10 Column", df.join(df_join,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"inner").collect())

// COMMAND ----------

/*** LEFT OUTER JOIN ***/
time("Column Operation, Left Outer Join 1 column", df.join(df_join_small,Seq("group0"),"left_outer").collect())


// COMMAND ----------

time("Column Operation, Left Outer Join 5 columns", df.join(df_join,Seq("group0","group1","group2","group3","group4"),"left_outer").collect())


// COMMAND ----------

time("Column Operation, Left Outer Join 10 columns", df.join(df_join,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"left_outer").collect())

// COMMAND ----------

/*** FULL OUTER JOIN ***/
time("Column Operation, Full Outer Join 1 column", df.join(df_join_small,Seq("group0"),"outer").collect())


// COMMAND ----------

time("Column Operation, Full Outer Join 5 columns", df.join(df_join,Seq("group0","group1","group2","group3","group4"),"outer").collect())


// COMMAND ----------

time("Column Operation, Full Outer Join 10 columns", df.join(df_join,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"outer").collect())

// COMMAND ----------

// DBTITLE 1,Column Operation: Mutate/Split
/* https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala */
time("Column Operation, Split 1 Column in 5",{
  df.withColumn("words0_0", split(col("words0"), "\\ |a|e|i|o|u").getItem(0))
     .withColumn("words0_1", split(col("words0"), "\\ |a|e|i|o|u").getItem(1))
     .withColumn("words0_2", split(col("words0"), "\\ |a|e|i|o|u").getItem(2))
     .withColumn("words0_3", split(col("words0"), "\\ |a|e|i|o|u").getItem(3))
     .withColumn("words0_4", split(col("words0"), "\\ |a|e|i|o|u").getItem(4))
     .collect()
     /*.select("words0_4").show(10)*/
})

// COMMAND ----------

time("Column Operation, Split 1 Column in 10",{
  df.withColumn("words0_0", split(col("words0"), "\\ |a|e|i|o|u").getItem(0))
     .withColumn("words0_1", split(col("words0"), "\\ |a|e|i|o|u").getItem(1))
     .withColumn("words0_2", split(col("words0"), "\\ |a|e|i|o|u").getItem(2))
     .withColumn("words0_3", split(col("words0"), "\\ |a|e|i|o|u").getItem(3))
     .withColumn("words0_4", split(col("words0"), "\\ |a|e|i|o|u").getItem(4))
     .withColumn("words0_5", split(col("words0"), "\\ |a|e|i|o|u").getItem(5))
     .withColumn("words0_6", split(col("words0"), "\\ |a|e|i|o|u").getItem(6))
     .withColumn("words0_7", split(col("words0"), "\\ |a|e|i|o|u").getItem(7))
     .withColumn("words0_8", split(col("words0"), "\\ |a|e|i|o|u").getItem(8))
     .withColumn("words0_9", split(col("words0"), "\\ |a|e|i|o|u").getItem(9))
     .collect()
     /*.select("words0_4").show(10)*/
})


// COMMAND ----------

// DBTITLE 1,Column Operation: Merge Columns
time("Merge 2 Columns in 1",df.withColumn("words0m1",concat(df("words0"), lit(" "),df("words1"))).collect())

// COMMAND ----------

time("Merge 5 Columns in 2",df.withColumn("words0m9",concat(
  df("words0"), lit(" "),df("words1"), lit(" "),df("words2"), lit(" ")
  ,df("words3"), lit(" "),df("words4")))
  .collect()
)

// COMMAND ----------

time("Merge 10 Columns in 2",df.withColumn("words0m9",concat(
  df("words0"), lit(" "),df("words1"), lit(" "),df("words2"), lit(" ")
  ,df("words3"), lit(" "),df("words4"), lit(" "),df("words5"), lit(" ")
  ,df("words6"), lit(" "),df("words7"), lit(" "),df("words8"), lit(" ")
  ,df("words9")))
  .collect()
)


// COMMAND ----------

// DBTITLE 1,Aggregate: Mathematical Operations by Group
time("Aggregate Operation, GroupBy 1 column",{
   df.select("Group0","Int1","Float1").groupBy("Group0").count().collect()
   df.select("Group0","Int1","Float1").groupBy("Group0").sum().collect()
   df.select("Group0","Int1","Float1").groupBy("Group0").avg().collect()
   df.select("Group0","Int1","Float1").groupBy("Group0").min().collect()
   df.select("Group0","Int1","Float1").groupBy("Group0").max().collect()
 })

// COMMAND ----------

 time("Aggregate Operation, GroupBy 5 columns",{
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").count().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").sum().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").avg().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").min().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").max().collect()
 })

// COMMAND ----------

time("Aggregate Operation, GroupBy 10 columns",{
   df.select("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9","Int1","Float1")
   .groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").count().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9","Int1","Float1")
   .groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").sum().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9","Int1","Float1")
   .groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").avg().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9","Int1","Float1")
   .groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").min().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9","Int1","Float1")
   .groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").max().collect()
 })


// COMMAND ----------

// DBTITLE 1,Aggregate: Ranking by Group

time("Aggregate Operation, Ranking by Group",
    df.select("Group0","Int1").withColumn("rank", dense_rank().over(Window.partitionBy("Group0").orderBy(desc("Int1")))).collect()
  )

// COMMAND ----------

// DBTITLE 1,Mixed: Melting


// COMMAND ----------

// DBTITLE 1,Mixed: Casting


// COMMAND ----------

// DBTITLE 1,Mixed Pivoting
time("Pivot 1 Rows and 1 Column",df.groupBy("group0").pivot("group10").sum("float0").collect())

// COMMAND ----------

time("Pivot 5 Rows and 1 Column",{
  df.groupBy("group0","group1","group2","group3","group4","group5")
  .pivot("group10")
  .sum("float1")
  .collect()
})

// COMMAND ----------

time("Pivot 10 Rows and 1 Columns",{
  df.groupBy("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9")
  .pivot("group10")
  .sum("float2")
  .collect()
})
