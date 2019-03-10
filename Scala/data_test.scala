// Getting Started with DataFrames!

// Most Important Link:
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset


// Start a simple Spark Session
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

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
    scala.tools.nsc.io.File("time_log.csv").appendAll(out)
    return(result)
  }

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
/*
import org.apache.spark.SparkFiles
spark.sparkContext.addFile("https://raw.githubusercontent.com/maxmoro/SMU-DB7330-Spark_python/master/data_rand_test.csv")
spark.read.csv(SparkFiles.get("file.csv"))
*/
scala.tools.nsc.io.File("time_log.csv").delete()

val df = spark.read.option("header","true").option("inferSchema","true").csv("data_rand.csv")
val df2 = df.limit(df.count().toInt/2) /*df2 is needed for join operations, we use half of df to allow some outer join*/

println("testing Time 1000ms")
time("test 1sec",{Thread.sleep(1000)})
/*********************/
/* COLUMN OPERATIONS */
/*********************/

/** Sorting **/
time("Sorting Ascending 1 column",{df.sort("words0").count()})
time("Sorting Ascending 5 columns",{df.sort("words0","words1","words2","words3","words4").count()})
time("Sorting Ascending 10 columns",{df.sort("words0","words1","words2","words3","words4","words5","words6","words7","words8","words9").count()})

time("Sorting Descending 1 column",{df.sort($"words0".desc).count()})
time("Sorting Descending 5 columns",{df.sort($"words0".desc,$"words1".desc,$"words2".desc,$"words3".desc,$"words4".desc).count()})
time("Sorting Descending 10 columns",{df.sort($"words0".desc,$"words1".desc,$"words2".desc,$"words3".desc,$"words4".desc,$"words5".desc,$"words6".desc,$"words7".desc,$"words8".desc,$"words9".desc).count()})

/** Joins **/
/* https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/DataFrame.html#join(org.apache.spark.sql.DataFrame,%20org.apache.spark.sql.Column,%20java.lang.String)*/
time("Inner Join 1 column", df.join(df2,df("group14")===df2("group14"),"inner").count())
time("Inner Join 5 columns", df.join(df2,Seq("group0","group1","group2","group3","group4"),"inner").count())
time("Inner Join 10 columns", df.join(df2,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"inner").count())

time("Left Outer Join 1 column", df.join(df2,df("group14")===df2("group14"),"left_outer").count())
time("Left Outer Join 5 columns", df.join(df2,Seq("group0","group1","group2","group3","group4"),"left_outer").count())
time("Left Outer Join 10 columns", df.join(df2,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"left_outer").count())

time("Full Outer Join 1 column", df.join(df2,df("group14")===df2("group14"),"outer").count())
time("Full Outer Join 5 columns", df.join(df2,Seq("group0","group1","group2","group3","group4"),"outer").count())
time("Full Outer Join 10 columns", df.join(df2,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"outer").count())

/** Mutate / Split Column **/
/* https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala */
time("Split 1 Column in 5",{
  df.withColumn("words0_0", split(col("words0"), "\\ |a|e|i|o|u").getItem(0))
     .withColumn("words0_1", split(col("words0"), "\\ |a|e|i|o|u").getItem(1))
     .withColumn("words0_2", split(col("words0"), "\\ |a|e|i|o|u").getItem(2))
     .withColumn("words0_3", split(col("words0"), "\\ |a|e|i|o|u").getItem(3))
     .withColumn("words0_4", split(col("words0"), "\\ |a|e|i|o|u").getItem(4))
     .count()
     /*.select("words0_4").show(10)*/
})
time("Split 1 Column in 10",{
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
     .count()
     /*.select("words0_4").show(10)*/
})

/* http://www.latkin.org/blog/2014/11/09/a-simple-benchmark-of-various-math-operations/
*/
time("Mathematical Operations 1 Column",df.withColumn("float0_1",sqrt(df("float0"))).count())
time("Mathematical Operations 5 Columns",{
  df.withColumn("float0_0",sqrt(abs(df("float0")+.001)))
    .withColumn("float1_0",sin(df("float1")))
    .withColumn("float2_0",tan(df("float2")))
    .withColumn("float3_0",sin(df("float3")))
    .withColumn("float4_0",cos(df("float4")))
    .agg(sum("float0_0"),sum("float1_0"),sum("float2_0"),sum("float3_0"),sum("float4_0"))
    .show()
    /*aggregation done to make sure Spark will calculate the new columns*/
})
time("Mathematical Operations 10 Columns",{
   df.withColumn("float0_0",sqrt(abs(df("float0"))))
    .withColumn("float1_0",sin(df("float1")))
    .withColumn("float2_0",tan(df("float2")))
    .withColumn("float3_0",sin(df("float3")))
    .withColumn("float4_0",cos(df("float4")))
    .withColumn("float5_0",log(sqrt(df("float5"))))
    .withColumn("float6_0",log(sin(cos(df("float6")))))
    .withColumn("float7_0",log(tan(df("float7"))))
    .withColumn("float8_0",log(sin(df("float7"))))
    .withColumn("float9_0",log(cos(df("float7"))))
    .agg(sum("float0_0"),sum("float1_0"),sum("float2_0"),sum("float3_0"),sum("float4_0")
        ,sum("float5_0"),sum("float6_0"),sum("float7_0"),sum("float8_0"),sum("float9_0"))
    .show()
    /*aggregation done to make sure Spark will calculate the new columns*/
})

/** Merge Columns **/
time("Merge 2 Columns in 1",df.withColumn("words0m1",concat(df("words0"), lit(" "),df("words1"))).count())
time("Merge 5 Columns in 2",df.withColumn("words0m9",concat(
  df("words0"), lit(" "),df("words1"), lit(" "),df("words2"), lit(" ")
  ,df("words3"), lit(" "),df("words4")))
  .count()
)
time("Merge 10 Columns in 2",df.withColumn("words0m9",concat(
  df("words0"), lit(" "),df("words1"), lit(" "),df("words2"), lit(" ")
  ,df("words3"), lit(" "),df("words4"), lit(" "),df("words5"), lit(" ")
  ,df("words6"), lit(" "),df("words7"), lit(" "),df("words8"), lit(" ")
  ,df("words9")))
  .count()
)

/*********************/
/* MIXED OPERATIONS */
/*********************/

/** Melting **/

/* MELTING and CASTING not avaialble in Spark, we can use PIVOT */
/*
https://www.mien.in/2018/03/25/reshaping-dataframe-using-pivot-and-melt-in-apache-spark-and-pandas/
*/

/** Picot **/
time("Pivot 1 Rows and 1 Column",df.groupBy("group0").pivot("group10").sum("float0").count())
time("Pivot 5 Rows and 1 Column",{
  df.groupBy("group0","group1","group2","group3","group4","group5")
  .pivot("group11")
  .sum("float0")
  .count()
})
time("Pivot 10 Rows and 1 Columns",{
  df.groupBy("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9")
  .pivot("group12")
  .sum("float0")
  .count()
})
