// Getting Started with DataFrames!

// Most Important Link:
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset


// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

def time[R](block: => R): R = {
    //based on  http://biercoff.com/easily-measuring-code-execution-time-in-scala/
    println("------------------------------------------------------")
    println("Begin Timing: " + System.currentTimeMillis() + "ms")
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("End Timing: " + System.currentTimeMillis() + "ms")
    println("Elapsed time: " + (t1 - t0) + "ms")
    println("------------------------------------------------------")
    result
}

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
/*
import org.apache.spark.SparkFiles
spark.sparkContext.addFile("https://raw.githubusercontent.com/maxmoro/SMU-DB7330-Spark_python/master/data_rand_test.csv")
spark.read.csv(SparkFiles.get("file.csv"))
*/

val df = spark.read.option("header","true").option("inferSchema","true").csv("data_rand.csv")
//val df = spark.read.option("header","true").option("inferSchema","true").csv("https://raw.githubusercontent.com/maxmoro/SMU-DB7330-Spark_python/master/data_rand_test.csv")
df.head(6)
df.printSchema()

println("testing Time 1000ms")
time{Thread.sleep(1000)}

time{df.groupBy("group1","group2").count().orderBy("group1","group2").show(1)} /* Scala is lazy */

time{df.groupBy("group1","group2").mean("int1","float19").orderBy("group1","group2").show(1)}
