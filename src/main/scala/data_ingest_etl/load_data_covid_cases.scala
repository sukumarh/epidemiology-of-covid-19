package data_ingest_etl

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{length, lit, lower}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utilities.utilities.printToConsole

import scala.collection.Map
import scala.collection.mutable.ListBuffer

object load_data_covid_cases {

  def load_globaldata_COVID(sc: SparkContext, spark: SparkSession, paths: Map[String, String]): DataFrame = {

    printToConsole("LOADING GLOBAL COVID 19 CASES DATASET")

    val format = new SimpleDateFormat("MM/dd/yyyy")

    val global_casesRDD = sc.textFile(paths("covid_cases_global")).map(_.split(","))

    val global_casesRDD_2 = global_casesRDD.map(line => Tuple7(format.parse(line(0)),
      line(4).toInt, line(5).toInt, line(6), line(7), line(8), 0))
      .groupBy(t => t._6).mapValues(_.toList)
      .mapValues(_.sortBy(_._1))
      .mapValues(t => next_peak(t))
      .values.flatMap(t=>t)

    val global_cases_row = global_casesRDD_2
      .map(line => Row(new java.sql.Date(line._1.getTime), line._2, line._3, line._4, line._5, line._6, line._7))

    val global_cases_df = SaveAsDF_Global(sc, spark, global_cases_row)

    printToConsole("GLOBAL COVID 19 CASES DATASET LOADED")

    global_cases_df
  }

  def loadData_COVID(sc: SparkContext, spark: SparkSession, paths: Map[String, String]): DataFrame = {

    import spark.implicits._

    printToConsole("LOADING COVID 19 CASES DATASET")

    val format = new SimpleDateFormat("MM/dd/yyyy")

    // Loading the CSV file containing the case count
    // Splitting the CSV format to obtain an RDD of arrays
    val casesRDD = sc.textFile(paths("covid_cases_us")).map(_.split(","))

    // Selecting the required columns
    // Normalizing the different fields of the tuple
    // Setting strings to lower case to avoid conflicts in join
    // Adding another value to the tuple, to be later mapped to the growth rate
    val casesRDD_filtered = casesRDD.map(line => Tuple7(format.parse(line(0))
      , line(1).toLowerCase, line(2).toLowerCase, line(3),
      line(4).toInt, line(5).toInt, 0))

    // Grouping the data based on the fips code, converting the values from Compact Buffers to Lists
    val casesRDD_groupedByFips = casesRDD_filtered.groupBy(t => t._4).mapValues(_.toList)

    // Sorting each list of every county by date which is the first field in each element (Tuple) of the list
    val casesRDD_groupedByFips_sortedByDate = casesRDD_groupedByFips.mapValues(_.sortBy(_._1))

    // Computing the growth rate for each fips code
    val casesRDD_growthCalculated = casesRDD_groupedByFips_sortedByDate.mapValues(t => calculateGrowth(t))

    // Converting the RDD to a DataFrame
    val casesRDD_flat = casesRDD_growthCalculated.values.flatMap(t => t)

    val casesRDD_in_rows = casesRDD_flat.map(line => Row(new java.sql.Date(line._1.getTime), line._2, line._3, line._4, line._5, line._6, line._7))

    val cases_DF = SaveAsDF(sc, spark, casesRDD_in_rows)

    val cases_DF_ = cases_DF
      .withColumn("fips_state", $"fips_code".substr(lit(0), length($"fips_code") - 3).cast("Int"))
      .withColumn("fips_county", $"fips_code".substr(lit(length($"fips_code") - 2), length($"fips_code")).cast("Int"))

    printToConsole("COVID 19 CASES DATASET LOADED")

    cases_DF_
  }

  // The calculateGrowth function is used to compute the growth rate of confirmed cases and assign the growth rate
  // to the last field of the Tuple. The new List is returned.
  def calculateGrowth(tuple_list: List[(Date, String, String, String, Int, Int, Int)]):
  List[(Date, String, String, String, Int, Int, Int)] = {
    val it = tuple_list.iterator
    var last_count = 0
    var resLB = ListBuffer[(Date, String, String, String, Int, Int, Int)]()
    while (it.hasNext) {
      val t = it.next()
      val t_updated = t.copy(_7 = t._5 - last_count)
      last_count = t._5
      resLB += t_updated
    }
    resLB.toList
  }

  def next_peak(tuple_list: List[(Date, Int, Int, String, String, String, Int)])={
    val it = tuple_list.iterator
    var resLB = ListBuffer[(Date, Int, Int, String, String, String, Int)]()
    while (it.hasNext){
      val t = it.next()
      val it_1 = it
      var count = 0
      var max = t._2
      if(it_1.hasNext){
        max = it_1.next()._2
      }
      while(it_1.hasNext && count<13){
        val t_1 = it_1.next()
        if(max < t_1._2){
          max = t_1._2
        }
        count = count + 1
      }
      val t_updated = t.copy(_7 = max)
      resLB += t_updated
    }
    resLB.toList
  }

  private def SaveAsDF_Global(sc: SparkContext, spark: SparkSession, global_cases_in_rows: RDD[Row]): DataFrame = {

    import spark.implicits._

    val schema = new StructType().add("Date", DateType).add("Cases", IntegerType)
      .add("Deaths", IntegerType).add("Country", StringType)
      .add("Geo_id", StringType)
      .add("Terr_code", StringType).add("Max_Peak",IntegerType)

    val global_casesDF_ = spark.createDataFrame(global_cases_in_rows, schema)

    val global_casesDF = global_casesDF_.withColumn("Country",lower($"Country"))
    println("Global Cases DF Schema")
    global_casesDF.printSchema

    global_casesDF
  }

  private def SaveAsDF(sc: SparkContext, spark: SparkSession, casesRDD_in_rows: RDD[Row]): DataFrame = {

    val schema = new StructType().add("Date", DateType).add("County", StringType)
      .add("State", StringType).add("fips_code", StringType)
      .add("Confirmed_Cases", IntegerType)
      .add("Deaths", IntegerType).add("Growth_Rate", IntegerType)

    val casesDF = spark.createDataFrame(casesRDD_in_rows, schema)

    println("Cases DF Schema")
    casesDF.printSchema

    casesDF
  }
}
