package data_ingest_etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{format_string, lower, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utilities.validator.correctCountyLabels

import scala.collection.Map

object load_demographic_data {

  def loadData_populationDensity(sc: SparkContext, spark: SparkSession, paths: Map[String, String]) = {

    import spark.implicits._

    val popDF_load = spark.read.format("csv")
      .option("header", "true").option("inferschema", "true")
      .load(paths("population_data"))

    val newNames = Seq("statecode", "countycode", "State", "County", "Population")

    val popDF_ = popDF_load.select("STATE", "COUNTY", "STNAME", "CTYNAME", "POPESTIMATE2019")
      .toDF(newNames: _*)
      .filter($"State" =!= $"County")
      .withColumn("State", lower($"State"))

    val popDF: DataFrame = correctCountyLabels(popDF_, "County")

    // popDF.show()

    val geoDF_load = spark.read.format("csv")
      .option("header", "true").option("inferschema", "true")
      .load(paths("geographic_data"))

    val geoDF = geoDF_load
      .select("census_block_group", "amount_land")
      .withColumn("census_block_group", format_string("%012d", $"census_block_group"))
      .withColumn("statecode", substring($"census_block_group", 0, 2).cast("int"))
      .withColumn("countycode", substring($"census_block_group", 3, 3).cast("int"))
      .groupBy("statecode", "countycode").sum("amount_land")
      .withColumnRenamed("sum(amount_land)", "amount_land")

    val pop_density_df = geoDF.join(popDF, Seq("statecode", "countycode"), "inner")
      .drop("statecode", "countycode")
      .withColumn("Population", $"Population".cast("Long"))
      .withColumn("amount_land", $"amount_land".cast("Long"))
      .withColumn("Population_Density", $"Population" * 10000/ $"amount_land")

    //pop_density_df.show()

    pop_density_df
  }


}
