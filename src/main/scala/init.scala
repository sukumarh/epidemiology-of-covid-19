import analytics.analytics.{climate_analytics, social_distancing_analytics, air_traffic_analytics}
import utilities.config.loadConfig
import utilities.utilities.{printToConsole, dropExistingTables}
import data_ingest_etl.load_data_air_traffic.loadData_airTraffic
import data_ingest_etl.load_data_climate.loadData_Climate
import data_ingest_etl.load_data_covid_cases.{loadData_COVID, load_globaldata_COVID}
import data_ingest_etl.load_data_social_distancing.loadData_socialDistancing
import data_ingest_etl.load_demographic_data.loadData_populationDensity
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import profiling.profiler.profile_datasets


import scala.collection.Map

object init {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    printToConsole("START")

    val conf = new SparkConf().setAppName("BDAD_Project").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

    val paths = loadConfig(false): Map[String, String]

    // dropExistingTables(spark)

    // Load Population Density dataset
    val pop_density_df = loadData_populationDensity(sc, spark, paths)

    // Load COVID cases dataset of US
    val cases_DF: DataFrame = loadData_COVID(sc, spark, paths)

    // Load global COVID cases dataset
    val global_cases_DF: DataFrame = load_globaldata_COVID(sc, spark, paths)

    // Load Social distancing dataset
    val socialDistance_DF = loadData_socialDistancing(sc, spark, paths, pop_density_df)

    // Load climate characteristics dataset
    val climate_DF = loadData_Climate(spark, paths)

    // Load air traffic
    val air_traffic_DF = loadData_airTraffic(spark, paths)

    profile_datasets(Array("COVID 19 cases", "Social Distancing", "Climate", "Air Traffic"),
      cases_DF, socialDistance_DF, climate_DF, air_traffic_DF)

    printToConsole("PERFORMING ANALYTICS")

    social_distancing_analytics(spark, cases_DF, socialDistance_DF)

    climate_analytics(cases_DF, climate_DF)

    air_traffic_analytics(spark, cases_DF, air_traffic_DF, global_cases_DF)

    printToConsole("END")

  }
}
