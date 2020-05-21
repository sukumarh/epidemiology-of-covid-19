package data_ingest_etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import utilities.utilities.{loadMultipleFiles, printToConsole}

import scala.collection.Map

object load_data_social_distancing {

  def loadData_socialDistancing(sc: SparkContext, spark: SparkSession, paths: Map[String, String], pop_density_df: DataFrame) = {

    printToConsole("LOADING SOCIAL DISTANCING DATASET")

    import spark.implicits._

    // Setup the file paths
    val filePaths = loadMultipleFiles(paths("social_distancing_us"))

   // Load the data into DataFrame from the file paths
    val load_df = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferschema", "true")
      .option("quote", "\"").option("escape", "\"").option("delimiter", ",")
      .load(filePaths: _*)

    // Add leading zeros to Census Block Group
    // Converting the format of date as required
    val load_df_dated = load_df
      .withColumn("origin_census_block_group", format_string("%012d", $"origin_census_block_group"))
      .withColumn("Date", to_date($"date_range_start", "yyyy-MM-dd"))
      .drop("date_range_start", "date_range_end")

    // Retrieving the state and county code from the Census Block Group
    val df_with_codes = load_df_dated
      .withColumn("fips_state", substring($"origin_census_block_group", 0, 2).cast("int"))
      .withColumn("fips_county", substring($"origin_census_block_group", 3, 3).cast("int"))

    // Converting the string to Array
    val df_formated = df_with_codes
      .withColumn("at_home_by_each_hour", split(col("at_home_by_each_hour"), ","))

    // Grouping based on
    // Date, State and County
    val df_grouped = df_formated
      .groupBy("Date", "fips_state", "fips_county")
      .sum("completely_home_device_count", "device_count", "distance_traveled_from_home", "full_time_work_behavior_devices", "part_time_work_behavior_devices")
      .withColumnRenamed("sum(completely_home_device_count)", "completely_home_device_count")
      .withColumnRenamed("sum(device_count)", "device_count")
      .withColumnRenamed("sum(distance_traveled_from_home)", "distance_traveled_from_home")
      .withColumnRenamed("sum(full_time_work_behavior_devices)", "full_time_work_behavior_devices")
      .withColumnRenamed("sum(part_time_work_behavior_devices)", "part_time_work_behavior_devices")

    // Calculating the percent of devices (population) at home, at work, at part time work
    val df_percent = df_grouped
      .withColumn("percent_at_home",
        (col("completely_home_device_count") / col("device_count") * 100))
      .withColumn("percent_full_time_work",
        (col("full_time_work_behavior_devices") / col("device_count") * 100))
      .withColumn("percent_part_time_work",
        (col("part_time_work_behavior_devices") / col("device_count") * 100))

    // Loading the FIPS Code mapping file to retrieve the State and County name
    val area_dict_df = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferschema", "true")
      .option("quote", "\"").option("escape", "\"").option("delimiter", ",")
      .load(paths("area_dictionary"))
      .withColumnRenamed("state_fips", "fips_state")
      .withColumnRenamed("county_fips", "fips_county")
      .withColumnRenamed("state", "State")
      .withColumnRenamed("county", "County")

    // Joining the social distance dataframe and the area_dictionary dataframe
    val df_with_location = area_dict_df.join(df_percent, Seq("fips_state", "fips_county"), "inner")

    val social_df = df_with_location.join(pop_density_df, Seq("State", "County"), "inner")
      .drop("amount_land", "class_code")
      .withColumn("Social_Dist_Factor_Home", $"percent_at_home" * $"Population_Density")
      .withColumn("Social_Dist_Factor_Full_Work", $"percent_full_time_work" * $"Population_Density")
      .withColumn("Social_Dist_Factor_Part_Work", $"percent_part_time_work" * $"Population_Density")

    println("Social Distancing DF Schema")
    social_df.printSchema

    printToConsole("SOCIAL DISTANCING DATASET LOADED")

    social_df
  }
}
