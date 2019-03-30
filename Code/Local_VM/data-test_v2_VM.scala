// Databricks notebook source
/*** NOTES ****/
/*
We need 11 groups (for the pivot)
*/

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.DataFrame
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
val spark = SparkSession.builder().getOrCreate()


// COMMAND ----------

// DBTITLE 1,Input Parameters
var num_of_runs = 5
var randomize = 1
var size="10MB"
var machine = 2 //1 max 2 nikhil

// COMMAND ----------

// DBTITLE 1,Setting Variables

var randomizeText = if(randomize == 1) "Random" else "Sequential"
var file_loc = ""
// UPDATE THIS PATH //
//var file_prefix = "/home/max/SMU-DB7330-Spark_python/blob/datasets/"
var file_prefix = "/home/nikhil/msds7330/datasets/"
var file_name = "dataset_" + size

val dateFormatter = new SimpleDateFormat("yyyyMMdd_hhmm")
val now  = dateFormatter.format(Calendar.getInstance().getTime())
// UPDATE THIS PATH //
//var logFileName ="/home/max/SMU-DB7330-Spark_python/blob/timelogs/time_Scala_" +randomizeText + "_" + file_name + "_" + now + ".csv"
var logFileName ="/home/nikhil/msds7330/SMU-DB7330-Spark_python/Results/Local_VM/machine2/time_Scala_" +randomizeText + "_" + file_name + "_" + now + ".csv"
println("Log File Name:" + logFileName)


// COMMAND ----------

// DBTITLE 1,Loading Data
println("loading data, START")
val df = spark.read.option("header","true").option("inferSchema","true").csv(file_prefix + file_name + ".csv")
val df_100 = spark.read.option("header","true").option("inferSchema","true").csv(file_prefix + file_name+ "_add_100.csv")
val df_1000 = spark.read.option("header","true").option("inferSchema","true").csv(file_prefix + file_name+ "_add_1000.csv")
val df_10000 = spark.read.option("header","true").option("inferSchema","true").csv(file_prefix + file_name+ "_add_10000.csv")
val df_join = df.limit(df.count().toInt/3) /*df2 is needed for join operations, we use half of df to allow some outer join*/
val df_join_small = df.limit(30)
println("loading data, DONE")

// COMMAND ----------

// DBTITLE 1,Timing Function
// Time function
def time[R](runID:Int=0,selType:Int=1,text:String, block: => R): R = {
    //based on  http://biercoff.com/easily-measuring-code-execution-time-in-scala/

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy hh:mm")
    val now  = dateFormatter.format(Calendar.getInstance().getTime())

    println("_____________________________________________________")
    println("** " + text + " **")
    //println("Begin Timing: " + System.currentTimeMillis() + "ms")
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    val tdiff =  (t1 - t0)/1000.0000
    //println("End Timing: " + System.currentTimeMillis() + "ms")
    println("Elapsed time: " + tdiff + "ms")
    /*println("_____________________________________________________")*/
    //var out  = {runID + "," + selType + "," +now + "," + text+ "," + t0 + "," + t1 + "," + tdiff+"\r\n"}
     var out  = {"Scala," + selType + "," + file_name + "," + machine + "," + runID + ","+ text + "," + tdiff+"\r\n"}
    //var filename = "/dbfs/mnt/blob/time_log_Scala.csv"
    println (out)
    scala.tools.nsc.io.File(logFileName).appendAll(out)
    return(result)
  }

// COMMAND ----------

// DBTITLE 1,Testing Item
// setup and Testing Time
scala.tools.nsc.io.File(logFileName).delete()
println("testing Time 1000ms")
//time(0,-1,"test, 1 second",{Thread.sleep(1000)})

// COMMAND ----------



// COMMAND ----------

// DBTITLE 1,Queries
  /**** Queries Function ****/
def queries(runID:Int=1,doQuery: Int=1,selType: Int=0){
  var timed =
      doQuery match {
        case 1 => {time(runID,selType,"Row Operation, Filter",df.filter($"int17" > 200 && $"float17"< 200).collect() )}
        case 2 => {var expr = "^overmeddled"
                   time(runID,selType,"Row Operation, Filter Reg Ex 1",df.filter($"Group0".rlike(expr)).collect())
                  }
        case 3 => {var expr = ".{2}lf.*s$"
                   time(runID,selType,"Row Operation, Filter Reg Ex 2",df.filter($"Group0".rlike(expr)).collect())
                  }
        case 4 => {var w = org.apache.spark.sql.expressions.Window.orderBy("Int1")
                   time(runID,selType,"Row Operation, Shift (Lag)",df.select("Int1").withColumn("status_lag", lag("Int1", 1, 0).over(w)).collect())
                  }
        case 5 => {var w2= org.apache.spark.sql.expressions.Window.orderBy("Int1")
                   time(runID,selType,"Row Operation, Running Sum", df.select("Int1").withColumn("CumSumTotal", sum("Int1") over (w2)).collect())
                  }

        case 6 => {time(runID,selType,"Row Operation, Writing 100 new rows", df.union(df_100).collect())}
        case 7 => {time(runID,selType,"Row Operation, Writing 1000 new rows", df.union(df_1000).collect())}
        case 8 => {time(runID,selType,"Row Operation, Writing 10000 new rows", df.union(df_10000).collect())}
        case 9 => {time(runID,selType,"Column Operation, Sorting Asc 1 column",df.sort("words0").collect())}
        case 10=> {time(runID,selType,"Column Operation, Sorting Asc 5 column",df.sort("words0","words1","words2","words3","words4").collect())}
        case 11 => {time(runID,selType,"Column Operation, Sorting Asc 10 column"
                         ,df.sort("words0","words1","words2","words3","words4","words5","words6","words7","words8","words9").collect())
                    }
        case 12 => {time(runID,selType,"Column Operation, Sorting Desc 1 column",df.sort($"words0".desc).collect())}
        case 13 => {time(runID,selType,"Column Operation, Sorting Desc 5 column"
                         ,df.sort($"words0".desc,$"words1".desc,$"words2".desc,$"words3".desc,$"words4".desc).collect())}
        case 14 => {time(runID,selType,"Column Operation, Sorting Desc 10 column"
                         ,df.sort($"words0".desc,$"words1".desc,$"words2".desc,$"words3".desc,$"words4".desc
                                  ,$"words5".desc,$"words6".desc,$"words7".desc,$"words8".desc,$"words9".desc).collect())
                   }
        case 15 => {time(runID,selType,"Column Operation, Mathematical Operations on Columns"
                         ,df.select("Int1","Float1","Int2","Float3","Float10")
                         .agg(sum("Int1"),avg("Float1"),count("Int2"),min("Float3"),max("Float10")).collect())
                   }

         case 16 => if(file_name == "dataset_10MB"){
                     time(runID,selType,"Column Operation, Inner Join 3 Columns", df.join(df_join,Seq("group0","group1","group2"),"inner").collect())
                   } else {println("removed due to memory limits")}
         case 17 => if(file_name == "dataset_10MB" | (file_name == "dataset_100MB")){
                     val df_join = df.limit(df.count().toInt/2)
                     time(runID,selType,"Column Operation, Inner Join 5 Columns"
                          , df.join(df_join,Seq("group0","group1","group2","group3","group4"),"inner").collect())
                    } else {println("removed due to memory limits")}
         case 18 => if(file_name == "dataset_10MB" | (file_name == "dataset_100MB")) {
                     time(runID,selType,"Column Operation, Inner Join 10 Columns",
                         df.join(df_join,Seq("group0","group1","group2","group3","group4"
                                             ,"group5","group6","group7","group8","group9"),"inner").collect())
                    } else {println("removed due to memory limits")}

         case 19 => if(file_name == "dataset_10MB"){
                   time(runID,selType,"Column Operation, Left Outer Join 3 Columns"
                          , df.join(df_join,Seq("group0","group1","group2"),"left_outer").collect())
                    } else {println("removed due to memory limits")}

         case 20 => if(file_name == "dataset_10MB" | (file_name == "dataset_100MB")) {
                     time(runID,selType,"Column Operation, Left Outer Join 5 Columns"
                          , df.join(df_join,Seq("group0","group1","group2","group3","group4"),"left_outer").collect())
                    }  else {println("removed due to memory limits")}

         case 21 => if(file_name == "dataset_10MB" | (file_name == "dataset_100MB")) {
                       time(runID,selType,"Column Operation, Left Outer Join 10 Columns"
                          ,df.join(df_join,Seq("group0","group1","group2","group3","group4"
                                              ,"group5","group6","group7","group8","group9"),"left_outer").collect())
                    } else {println("removed due to memory limits")}

         case 22 =>  if(file_name == "dataset_10MB"){
                    time(runID,selType,"Column Operation, Full Outer Join 3 Columns",
                          df.join(df_join,Seq("group0","group1","group2"),"outer").collect())

                    } else {println("removed due to memory limits")}

         case 23 => if(file_name == "dataset_10MB" | (file_name == "dataset_100MB")) {
                     time(runID,selType,"Column Operation, Full Outer Join 5 Columns",
                          df.join(df_join,Seq("group0","group1","group2","group3","group4"),"outer").collect())
                    }  else {println("removed due to memory limits")}

         case 24 => if(file_name == "dataset_10MB" | (file_name == "dataset_100MB")) {
                     time(runID,selType,"Column Operation, Full Outer Join 10 Columns",
                          df.join(df_join,Seq("group0","group1","group2","group3","group4"
                                              ,"group5","group6","group7","group8","group9"),"outer").collect())
                    }  else {println("removed due to memory limits")}

        case 25 => {time(runID,selType,"Column Operation, Split 1 Column into 5",
                      df.withColumn("words0_0", split(col("words0"),  "\\ |a|e|i|o|u").getItem(0))
                      .withColumn("words0_1", split(col("words0"), "\\ |a|e|i|o|u").getItem(1))
                      .withColumn("words0_2", split(col("words0"), "\\ |a|e|i|o|u").getItem(2))
                      .withColumn("words0_3", split(col("words0"), "\\ |a|e|i|o|u").getItem(3))
                      .withColumn("words0_4", split(col("words0"), "\\ |a|e|i|o|u").getItem(4))
                      .collect())
                   }
        case 26 => {time(runID,selType,"Column Operation, Split 1 Column into 10",
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
                     .collect())
                   }
        case 27 => {time(runID,selType,"Column Operation, Merge 2 columns into 1"
                         ,df.withColumn("words0m1",concat(df("words0"), lit(" "),df("words1"))).collect())}
        case 28 => {time(runID,selType,"Column Operation, Merge 5 columns into 1"
                         ,df.withColumn("words0m9",concat(df("words0"), lit(" "),df("words1"), lit(" "),df("words2")
                                                          , lit(" "),df("words3"), lit(" "),df("words4")))
                         .collect())
                   }
        case 29 => {time(runID,selType,"Column Operation, Merge 10 columns into 1"
                         ,df.withColumn("words0m9",concat(df("words0"), lit(" "),df("words1"), lit(" "),df("words2")
                                                          , lit(" "),df("words3"), lit(" "),df("words4"), lit(" "),df("words5")
                                                          , lit(" "),df("words6"), lit(" "),df("words7"), lit(" "),df("words8")
                                                          , lit(" "),df("words9")))
                         .collect())
                   }
        case 30 => {time(runID,selType,"Aggregate Operations, GroupBy 1 column"
                         ,{df.groupBy("Group0").count().collect()
                           df.groupBy("Group0").sum().collect()
                           df.groupBy("Group0").avg().collect()
                           df.groupBy("Group0").min().collect()
                           df.groupBy("Group0").max().collect()
                          })
                   }
        case 31 => {time(runID,selType,"Aggregate Operations, GroupBy 5 columns"
                         ,{df.groupBy("Group0","Group2","Group4","Group6","Group8").count().collect()
                           df.groupBy("Group0","Group2","Group4","Group6","Group8").sum().collect()
                           df.groupBy("Group0","Group2","Group4","Group6","Group8").avg().collect()
                           df.groupBy("Group0","Group2","Group4","Group6","Group8").min().collect()
                           df.groupBy("Group0","Group2","Group4","Group6","Group8").max().collect()
                          })
                   }
        case 32 => {time(runID,selType,"Aggregate Operations, GroupBy 10 columns"
                         ,{df.groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").count().collect()
                           df.groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").sum().collect()
                           df.groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").avg().collect()
                           df.groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").min().collect()
                           df.groupBy("Group0","Group2","Group4","Group6","Group8","Group1","Group3","Group5","Group7","Group9").max().collect()
                          })
                   }
        case 33 => {time(runID,selType,"Aggregate Operations, Ranking by Group"
                         ,df.withColumn("rank", dense_rank().over(Window.partitionBy("Group0").orderBy(desc("Int1")))).collect())
                   }
        case 34 => {time(runID,selType,"Mixed Operation, Pivot 1 Rows and 1 Column"
                         ,df.groupBy("group0").pivot("group10").sum("float0").collect())
                   }
        case 35 => {time(runID,selType,"Mixed Operation, Pivot 5 Rows and 1 Column"
                         ,df.groupBy("group0","group1","group2","group3","group4").pivot("group10").sum("float1").collect())
                   }
        case 36 => {time(runID,selType,"Mixed Operation, Pivot 10 Rows and 1 Column"
                         ,df.groupBy("group0","group1","group2","group3","group4","group5","group6","group7","group8","group9")
                          .pivot("group10").sum("float2").collect())
                   }
        case whoa  => println("Unexpected case: " + whoa.toString)
      }
}

//queries(1,2,"test")
//runQueries(0,0)


// COMMAND ----------

// DBTITLE 1,Query Selector Function
def runQueries(runID:Int = 1
               ,randomize:Int = 0
                              ): Int = {
  import scala.util.Random
  import org.apache.spark.sql.functions.lag
  var numQueries = 36
  var qList = 1.to(numQueries)
  // https://stackoverflow.com/questions/32932229/how-to-randomly-sample-from-a-scala-list-or-array/32932871
  val doList = if(randomize==1) {Random.shuffle(qList.toList).take(numQueries)} else qList
  println(doList)
  var doQuery= 0
  println("******* Executing RunID ", runID," **********")
  for(doQuery <- doList.toList){
    var timed = queries(runID,doQuery,randomize)
  }
  1
}

// COMMAND ----------

// DBTITLE 1,Run Queries
println("Log File Name:" + logFileName)
var runID = 1
for (runID <- 1 to (num_of_runs+1)){

  runQueries(runID,randomize)
}


// COMMAND ----------

// COMMAND ----------

/*
dbutils.fs.mv("dbfs:/mnt/blob/dataset_1GB.csv", "dbfs:/mnt/blob/datasets/.")
dbutils.fs.rm("dbfs:/mnt/blob/datasets/dataset_1GB.csv")

val fileNm = dbutils.fs.ls("/mnt/blob/").map(_.name).filter(r => r.endsWith("csv"))(0)
val fileLoc = "dbfs:/mnt/blob/" + fileNm
dbutils.fs.rm(fileLoc)
//dbutils.fs.mv(fileLoc, "dbfs:/user/abc/Test/Test.csv")
*/

// COMMAND ----------


//display(dbutils.fs.ls("dbfs:/mnt/blob/timelogs"))
//display(dbutils.fs.ls("dbfs:/mnt/blob/datasets"))
//dbutils.fs.rm("dbfs:/FileStore/tables/DataGenerator_v1p1.ipynb")


// COMMAND ----------
