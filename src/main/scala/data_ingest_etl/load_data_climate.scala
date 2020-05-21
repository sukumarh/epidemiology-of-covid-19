package data_ingest_etl

import org.apache.spark.sql.functions.{col, lower, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utilities.utilities.printToConsole

import scala.collection.Map

object load_data_climate {

  def loadData_Climate(spark:SparkSession, paths: Map[String, String]) ={

    import spark.implicits._

    printToConsole("LOADING CLIMATE DATASET")

    val climateDF = spark.read.format("csv")
      .option("header", "true").option("inferschema", "true")
      .load(paths("climate"))

    val climateDF_us: DataFrame = climateDF.filter(climateDF("Country/Region") === "US")

    val columns_to_consider:Seq[String] = Seq("time", "Province/State",
      "precipIntensity","windSpeed",
      "temperatureHigh", "temperatureLow")

    val climate_df = climateDF_us.select(columns_to_consider.head, columns_to_consider.tail: _*)
      .withColumn("time", to_date($"time", "yyyy-MM-dd"))
      .withColumnRenamed("time", "Date")
      .withColumnRenamed("Province/State", "State")
      .withColumnRenamed("precipIntensity", "Precipitation")
      .withColumn("Avg_Temp", $"temperatureHigh" + $"temperatureLow" / 2)
      .withColumn("State", lower(col("State")))

    println("Climate DF Schema")
    climate_df.printSchema

    //println("Climate DF Show")
    //climate_df.show()

    printToConsole("CLIMATE DATASET LOADED")
    climate_df

  }

}
