package profiling

import breeze.linalg.min
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import utilities.utilities.printToConsole

object profiler {

  def profile_datasets(dataFrame_names: Array[String], data_frames: DataFrame*) = {

    printToConsole("Profiling Datasets")

    for (i <- data_frames.indices) {
      println("Dataset: " + dataFrame_names(i))
      val data_frame = data_frames(i)
      val selectColumns = data_frame.dtypes
      var removeColumns = ListBuffer[String]()
      for (x <- selectColumns) {
        println("| Column: " + "%40s".format(x._1) + " |  Distinct Count: " + "%20s".format(data_frame.select(x._1).distinct.count.toString + "|"))
        if (x._2.contains("String")) {
          removeColumns += x._1
        }
      }
      println("DataFrame described")
      val df = data_frame.drop(removeColumns: _*)

      if (df.columns.length > 5){
        val columns = df.columns
        var i = 0

        while (i < df.columns.length) {
          val c = columns.slice(i, min(i + 5, df.columns.length)).toSeq
          df.select(c.head, c.tail:_*).describe().show()
          i += 5
        }
      } else {
        data_frame.drop(removeColumns: _*).describe().show()
      }
    }
  }
}