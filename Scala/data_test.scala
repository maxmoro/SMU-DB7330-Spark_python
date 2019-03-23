// Getting Started with DataFrames!

// Most Important Link:
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset


// Start a simple Spark Session

import org.apache.spark.sql.SparkSession
/*
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
val conf = {new SparkConf()
             .setMaster("local[2]")
             .setAppName("TestingScala")
             .set("spark.executor.memory", "6g")
           }
val sc = new SparkContext(conf)
*/
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

val df = spark.read.option("header","true").option("inferSchema","true").csv("../DataGenerator/Data/data_rand.csv")
val df2 = df.limit(df.count().toInt/2) /*df2 is needed for join operations, we use half of df to allow some outer join*/

println("testing Time 1000ms")
time("test 1sec",{Thread.sleep(1000)})
/*********************/
/*  ROW OPERATIONS   */
/*********************/

// Filtering rows based on regular expressions
var expr = "^troubleshot"  //var expr = "^troubleshot"
time("Row Operation, Filter Reg Ex 1",df.filter($"Group0".rlike(expr)).collect())
// rr at 3rd and 4th position and ending with w
expr = ".{2}rr.*w$"
time("Row Operation, Filter Reg Ex 2",df.filter($"Group0".rlike(expr)).collect())

/* Shift (Lag) */
var w = org.apache.spark.sql.expressions.Window.orderBy("Int1")
import org.apache.spark.sql.functions.lag
time("Row Operation, Shift (Lag)",df.select("Int1").withColumn("status_lag", lag("Int1", 1, 0).over(w)).collect())

/** Running Sum **/
// https://stackoverflow.com/questions/46982119/how-to-calculate-cumulative-sum-in-a-pyspark-table
var w2= org.apache.spark.sql.expressions.Window.orderBy("Int1")
import org.apache.spark.sql.functions.lag
time("Row Operation, Running Sum",df.select("Int1").withColumn("CumSumTotal", sum("Int1").over(w2)).collect())

/** Writing New Rows **/
var df_100 = df.limit(100)
time("Row Operation, Writing 100 new rows", df.union(df_100).collect())
var df_1000 = df.limit(1000)
time("Row Operation, Writing 100 new rows", df.union(df_1000).collect())
var df_10000 = df.limit(10000)
time("Row Operation, Writing 100 new rows", df.union(df_10000).collect())

/*********************/
/* COLUMN OPERATIONS */
/*********************/

/** Sorting **/
time("Column Operation, Sorting Asc 1 column",{df.sort("words0").collect()})
time("Column Operation, Sorting Asc 5 column",{df.sort("words0","words1","words2","words3","words4").collect()})
time("Column Operation, Sorting Asc 10 column",{df.sort("words0","words1","words2","words3","words4","words5","words6","words7","words8","words9").collect()})

time("Column Operation, Sorting Desc 1 column",{df.sort($"words0".desc).count()})
time("Column Operation, Sorting Desc 5 column",{df.sort($"words0".desc,$"words1".desc,$"words2".desc,$"words3".desc,$"words4".desc).collect()})
time("Column Operation, Sorting Desc 10 column",{df.sort($"words0".desc,$"words1".desc,$"words2".desc,$"words3".desc,$"words4".desc,$"words5".desc,$"words6".desc,$"words7".desc,$"words8".desc,$"words9".desc).collect()})

/** Joins **/
/* https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/DataFrame.html#join(org.apache.spark.sql.DataFrame,%20org.apache.spark.sql.Column,%20java.lang.String)*/
/*time("Column Operation, Inner Join 1 Column", df.join(df2,df("group14")===df2("group14"),"inner").collect())  #Memory Overload*/
time("Column Operation, Inner Join 5 Column", df.join(df2,Seq("group0","group1","group2","group3","group4"),"inner").collect())
time("Column Operation, Inner Join 10 Column", df.join(df2,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"inner").collect())

/*time("Column Operation, Left Outer Join 1 column", df.join(df2,df("group14")===df2("group14"),"left_outer").collect())*/
time("Column Operation, Left Outer Join 5 columns", df.join(df2,Seq("group0","group1","group2","group3","group4"),"left_outer").collect())
time("Column Operation, Left Outer Join 10 columns", df.join(df2,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"left_outer").collect())

/*time("Column Operation, Full Outer Join 1 column", df.join(df2,df("group14")===df2("group14"),"outer").collect())*/
time("Column Operation, Full Outer Join 5 columns", df.join(df2,Seq("group0","group1","group2","group3","group4"),"outer").collect())
time("Column Operation, Full Outer Join 10 columns", df.join(df2,Seq("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9"),"outer").collect())

/** Mutate / Split Column **/
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

/* Column Operation: Mathematical Operations on Column */
time("Column Operation, Mathematical Operation on Columns",
  df.select("Int1","Float1","Int2","Float3","Float10").agg(sum("Int1"),avg("Float1"),count("Int2"),min("Float3"),max("Float10")).collect()
)

/* http://www.latkin.org/blog/2014/11/09/a-simple-benchmark-of-various-math-operations/
*/
/* Mathematical oprations on fields*/
time("Mathematical Operations 1 Column",df.withColumn("float0_1",sqrt(df("float0"))).collect())
time("Mathematical Operations 5 Columns",{
  df.withColumn("float0_0",sqrt(abs(df("float0")+.001)))
    .withColumn("float1_0",sin(df("float1")))
    .withColumn("float2_0",tan(df("float2")))
    .withColumn("float3_0",sin(df("float3")))
    .withColumn("float4_0",cos(df("float4")))
    /*.agg(sum("float0_0"),sum("float1_0"),sum("float2_0"),sum("float3_0"),sum("float4_0"))*/
    .collect()
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
    /*.agg(sum("float0_0"),sum("float1_0"),sum("float2_0"),sum("float3_0"),sum("float4_0")
        ,sum("float5_0"),sum("float6_0"),sum("float7_0"),sum("float8_0"),sum("float9_0"))
        */
    .collect()
})
/* Aggregate: Mathematical Operations by Group */
 time("Aggregate Operation, GroupBy 1 column",{
   df.select("Group0","Int1","Float1").groupBy("Group0").count().collect()
   df.select("Group0","Int1","Float1").groupBy("Group0").sum().collect()
   df.select("Group0","Int1","Float1").groupBy("Group0").avg().collect()
   df.select("Group0","Int1","Float1").groupBy("Group0").min().collect()
   df.select("Group0","Int1","Float1").groupBy("Group0").max().collect()
 })
 time("Aggregate Operation, GroupBy 5 columns",{
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").count().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").sum().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").avg().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").min().collect()
   df.select("Group0","Group2","Group4","Group6","Group8","Int1","Float1").groupBy("Group0","Group2","Group4","Group6","Group8").max().collect()
 })
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

/** Aggregate: Ranking by Group **/
ta.select(["Group0","Int1"]).withColumn("rank", dense_rank().over(Window.partitionBy("Group0").orderBy(desc("Int1")))).collect()
/** Merge Columns **/
time("Merge 2 Columns in 1",df.withColumn("words0m1",concat(df("words0"), lit(" "),df("words1"))).collect())
time("Merge 5 Columns in 2",df.withColumn("words0m9",concat(
  df("words0"), lit(" "),df("words1"), lit(" "),df("words2"), lit(" ")
  ,df("words3"), lit(" "),df("words4")))
  .collect()
)
time("Merge 10 Columns in 2",df.withColumn("words0m9",concat(
  df("words0"), lit(" "),df("words1"), lit(" "),df("words2"), lit(" ")
  ,df("words3"), lit(" "),df("words4"), lit(" "),df("words5"), lit(" ")
  ,df("words6"), lit(" "),df("words7"), lit(" "),df("words8"), lit(" ")
  ,df("words9")))
  .collect()
)

/*********************/
/* MIXED OPERATIONS */
/*********************/

/** Melting **/

/* MELTING and CASTING not avaialble in Spark, we can use PIVOT */
/*
https://www.mien.in/2018/03/25/reshaping-dataframe-using-pivot-and-melt-in-apache-spark-and-pandas/
*/

/** Pivot **/
time("Pivot 1 Rows and 1 Column",df.groupBy("group0").pivot("group10").sum("float0").count())
time("Pivot 5 Rows and 1 Column",{
  df.groupBy("group0","group1","group2","group3","group4","group5")
  .pivot("group11")
  .sum("float1")
  .collect()
})
time("Pivot 10 Rows and 1 Columns",{
  df.groupBy("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9")
  .pivot("group12")
  .sum("float2")
  .collect()
})
