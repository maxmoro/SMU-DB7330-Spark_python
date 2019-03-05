// Getting Started with DataFrames!

// Most Important Link:
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset


// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset

val df = spark.read.option("header","true").option("inferSchema","true").csv("data_rand.csv")
df.head(6)
df.printSchema()
