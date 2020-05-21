package utilities

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max, min}

import scala.collection.mutable.ListBuffer

object utilities {

  def loadMultipleFiles(directory: String): ListBuffer[String] = {
    val fs = FileSystem.get(new Configuration())
    val files = fs.listFiles(new Path(directory), true)
    val filePaths = new ListBuffer[String]
    while (files.hasNext()) {
      val file = files.next()
      filePaths += file.getPath.toString
    }

    filePaths
  }

  def printToConsole(text:String):Unit={
    println("---------------------------------------")
    println(text)
    println("---------------------------------------\n")
  }

  def dropExistingTables(spark: SparkSession):Unit = {
    spark.sql("DROP TABLE IF EXISTS casesDF")
    spark.sql("DROP TABLE IF EXISTS granger_result_table")
    spark.sql("DROP TABLE IF EXISTS Cases_and_SD")
    spark.sql("DROP TABLE IF EXISTS Cases_and_Climate")
  }

  def normalize(df: DataFrame, c: String): DataFrame = {
    val f = df.select(max(c), min(c)).first()
    val (max_, min_): (Double, Double) = (f.get(0).toString.toDouble, f.get(1).toString.toDouble)
    val normalized_df = df
      .withColumn(c + "_norm", (((col(c) - min_) / (max_ - min_)) * 1000).cast("Double"))
    normalized_df
  }
}
