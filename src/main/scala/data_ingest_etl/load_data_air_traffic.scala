package data_ingest_etl

import org.apache.spark.sql.functions.{lower, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utilities.utilities.{loadMultipleFiles, printToConsole}

import scala.collection.Map

object load_data_air_traffic {

  def loadData_airTraffic(spark:SparkSession, paths: Map[String, String]):DataFrame = {

    import spark.implicits._

    printToConsole("LOADING AIR TRAFFIC DATASET")

    // Setup the file paths
    val filePaths = loadMultipleFiles(paths("air_traffic"))

    val airDF = spark.read.format("csv")
      .option("header", "true").option("inferschema", "true")
      .load(filePaths: _*)

    val air_df = airDF.select("origin", "destination", "day")
      .withColumn("day", to_date($"day", "yyyy-MM-dd"))
      .withColumnRenamed("day", "date")
      .na.drop()

    val airport_codes_df = spark.read.format("csv")
      .option("header", "true").option("inferschema", "true")
      .load(paths("airport_codes"))

    val airports = airport_codes_df
      .select("ident","type", "name", "latitude_deg", "longitude_deg", "iso_country", "iso_region")

    val air_df_with_origin = air_df.join(airports, air_df("origin") === airports("ident"), "inner")

    //ISO_Code to country name mapping
    val country_mapping_df_ = spark.read.format("csv")
      .option("header", "true").option("inferschema", "true")
      .load(paths("countries_mapping"))

    //Selecting ISO_Code and Country name from the dataframe
    val country_mapping_df = country_mapping_df_.select("name","alpha-2")
      .withColumnRenamed("alpha-2","iso_country")
      .withColumnRenamed("name","Country")

    val air_traffic_with_origin = air_df_with_origin
    .drop("ident")
    .withColumnRenamed("name", "origin_airport_name")
    .withColumnRenamed("latitude_deg", "origin_latitude_deg")
    .withColumnRenamed("longitude_deg", "origin_longitude_deg")
    .withColumnRenamed("type", "origin_type")
    .withColumnRenamed("iso_region", "origin_region")
      .join(country_mapping_df,Seq("iso_country"),"inner")
      .withColumnRenamed("iso_country", "origin_iso_country")
      .withColumn("Country",lower($"Country"))
      .withColumnRenamed("Country", "origin_country")

    val air_df_with_des = air_traffic_with_origin
      .join(airports, air_df("destination") === airports("ident"), "inner")

    val air_traffic_df = air_df_with_des
      .drop("ident")
      .withColumnRenamed("name", "destination_airport_name")
      .withColumnRenamed("latitude_deg", "destination_latitude_deg")
      .withColumnRenamed("longitude_deg", "destination_longitude_deg")
      .withColumnRenamed("type", "destination_type")
      .withColumnRenamed("iso_region", "destination_region")
      .join(country_mapping_df,Seq("iso_country"),"inner")
      .withColumnRenamed("iso_country", "destination_iso_country")
      .withColumn("Country",lower($"Country"))
      .withColumnRenamed("Country", "destination_country")

    println("Air Traffic DF Schema")
    air_traffic_df.printSchema

    //println("Air Traffic DF Show")
    //air_traffic_df.show()

    // air_traffic_df.write.saveAsTable("Air_Traffic")

    printToConsole("AIR TRAFFIC DATASET LOADED")
    air_traffic_df

  }

}
