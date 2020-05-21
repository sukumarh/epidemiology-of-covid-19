package utilities

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object validator {

  // The validateCounties function will validate whether the counties in the input data are coherent with the
  // counties mapping dictionary
  // Since counties will be used along with date when joining multiple datasets, it is important to validate
  // them in order to avoid issues during join.
  def validateCounties(sc: SparkContext, counties_from_input: RDD[String], county_dictionary_path: String): Unit = {
    val county_dictionary = sc.textFile(county_dictionary_path).map(t => (t.split(",")(0).toLowerCase, 1))
    val counties_from_input_d = counties_from_input.distinct
    val input = counties_from_input_d.map(t => (t, 1))
    val joined = input.leftOuterJoin(county_dictionary)
    val errors = joined.filter(t => t._2._2.isEmpty)
    if (errors.count > 0) {
      println("Counties data validated. Issues found.")
      if (errors.count > 10) {
        println("Printing top 10.")
        errors.take(10).foreach(println)
      } else {
        println("Printing the errors.")
        errors.collect().foreach(println)
      }
    } else {
      println("Counties data validated. No validation issues.")
    }
  }

  def correctCountyLabels(df: DataFrame, c: String) = {
    val df_ = df
      .withColumn(c, lower(col(c)))
      .withColumn(c,
        when(col(c).contains(" county"), col(c).substr(lit(1), length(col(c)) - 7))
          .when(col(c).contains(" borough"), col(c).substr(lit(1), length(col(c)) - 8))
          .otherwise(col(c)))
    df_
  }
}
