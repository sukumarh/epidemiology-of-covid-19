package analytics

import GrangerCausality.testCausality
import utilities.utilities.normalize
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object analytics {

  def social_distancing_analytics(spark: SparkSession, cases_DF: DataFrame, social_distancing_DF: DataFrame) = {

    val sd_df = social_distancing_DF.drop("State", "County")

    val joined_with_sd = cases_DF.join(sd_df, Seq("Date", "fips_state", "fips_county"), "inner")

    // joined_with_sd.write.saveAsTable("Cases_and_SD")

    val normalized_df =
      normalize(
        normalize(
          normalize(
            normalize(joined_with_sd,
              "Growth_Rate"),
            "Social_Dist_Factor_Home"),
          "Social_Dist_Factor_Full_Work"),
        "Social_Dist_Factor_Part_Work")

    val analytics_county_level = {
      normalized_df
        .groupBy("fips_code")
        .agg(
          collect_list("Date"),
          collect_list("Growth_Rate_norm"),
          collect_list("Social_Dist_Factor_Home_norm"),
          collect_list("Social_Dist_Factor_Full_Work_norm"),
          collect_list("Social_Dist_Factor_Part_Work_norm")
        )
        .withColumnRenamed("collect_list(Date)", "Date")
        .withColumnRenamed("collect_list(Growth_Rate_norm)",
          "Growth_Rate_norm")
        .withColumnRenamed("collect_list(Social_Dist_Factor_Home_norm)",
          "Social_Dist_Factor_Home_norm")
        .withColumnRenamed("collect_list(Social_Dist_Factor_Full_Work_norm)",
          "Social_Dist_Factor_Full_Work_norm")
        .withColumnRenamed("collect_list(Social_Dist_Factor_Part_Work_norm)",
          "Social_Dist_Factor_Part_Work_norm")
    }

    def udf_granger =
      udf((x: mutable.WrappedArray[Double], y: mutable.WrappedArray[Double]) =>
        testGranger(x.toArray, y.toArray))

    println("Computing Causation of Growth Rate from Social Distancing Factor Home")
    val first_test = test_on_columns(analytics_county_level, "Social_Dist_Factor_Home_norm", "Growth_Rate_norm",
      spark, udf_granger)

    println("Computing Causation of Growth Rate from Social Distancing Factor Full Work")
    val second_test = test_on_columns(first_test, "Social_Dist_Factor_Full_Work_norm", "Growth_Rate_norm",
      spark, udf_granger)

    println("Computing Causation of Growth Rate from Social Distancing Part Work")
    val third_test = test_on_columns(second_test, "Social_Dist_Factor_Part_Work_norm", "Growth_Rate_norm",
      spark, udf_granger)

    val granger_result_table = third_test
      .drop("Date")
      .drop("Growth_Rate_norm")
      .drop("Social_Dist_Factor_Home_norm")
      .drop("Social_Dist_Factor_Full_Work_norm")
      .drop("Social_Dist_Factor_Part_Work_norm")
      .drop("Causation_computation")
      .drop("_tmp")

    // granger_result_table.write.saveAsTable("granger_result_table")
  }

  def air_traffic_analytics(spark:SparkSession, cases_DF: DataFrame, air_traffic_df: DataFrame, global_cases_df:DataFrame) = {

    import spark.implicits._

    val air_traffic_cases_df_ = global_cases_df.join(air_traffic_df,global_cases_df("Date") === air_traffic_df("date") && air_traffic_df("origin_country") === global_cases_df("Country"))

    val air_traffic_cases_df = air_traffic_cases_df_.filter(air_traffic_cases_df_("destination_iso_country") === "US")
      .withColumn("destination_region", substring($"destination_region",4,2))

    println("Computing Causation of Growth Rate from Air Traffic")

  }

  def climate_analytics(cases_DF: DataFrame, climate_DF: DataFrame) = {

    // Join based on Date and region
    val cases_DF_grouped = {
      cases_DF
        .groupBy("Date", "State")
        .sum("Growth_Rate", "Confirmed_Cases", "Deaths")
        .withColumnRenamed("sum(Growth_Rate)", "growth_rate")
        .withColumnRenamed("sum(Confirmed_Cases)", "confirmed_cases")
        .withColumnRenamed("sum(Deaths)", "deaths")
        .filter(col("growth_rate") =!= 0)
    }

    val joined_with_climate = cases_DF_grouped.join(climate_DF, Seq("Date", "State"), "inner")

    // joined_with_climate.write.saveAsTable("Cases_and_Climate")

    val x: Array[Double] = joined_with_climate
      .select("Avg_Temp").rdd.map(r => r(0).toString.toDouble).collect().toArray

    val y: Array[Double] = joined_with_climate
      .select("Growth_Rate").rdd.map(r => r(0).toString.toDouble).collect().toArray

    println("Computing Causation of Growth Rate from Temperature")
    testGranger(joined_with_climate, "Avg_Temp", "Growth_Rate", 3)
  }

  def test_on_columns(df: DataFrame, x: String, y: String, spark: SparkSession, udf_granger: UserDefinedFunction): DataFrame = {
    import spark.implicits._

    val df_ = df
      .withColumn("Causation_computation",
        udf_granger(col(x), col(y)))
      .withColumn("_tmp", split($"Causation_computation", "\\,"))

    val df__ = df_
      .select(
        df_.col("*"),
        $"_tmp".getItem(0).as("Causation_" + x + "_F"),
        $"_tmp".getItem(1).as("Causation_" + x + "_p")
      )

    df__
  }

  def testGranger(x: Array[Double], y: Array[Double]): String = {
    var granger_test_result: (Double, Double) = (1, 1)
    for (i <- 1 to 14) {
      try {
        val test_result = testCausality(x, y, i)
        if (test_result._2 < granger_test_result._2) {
          granger_test_result = test_result
        }
      } catch {
        case e: Exception => print("")
      }
    }
    granger_test_result._1 + "," + granger_test_result._2
  }

  def testGranger(df: DataFrame, col1: String, col2: String, lag: Int): Unit = {
    val x: Array[Double] = df
      .select(col1).rdd.map(r => r(0).toString.toDouble).collect().toArray

    val y: Array[Double] = df
      .select(col2).rdd.map(r => r(0).toString.toDouble).collect().toArray

    for (i <- 1 to 14) {
      try {
        testCausality(x, y, i)
      } catch {
        case e: Exception => print("")
      }
    }
  }
}
