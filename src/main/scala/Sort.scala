/*
* Sort workload for BigDataBench
*/
package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import BigDataBenchConf._

object Sort {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: Sort <master> <data_file> <save_file>" +
        " [<slices>]")
      System.exit(1)
    }

    val host = args(0)
    var splits = 1
    val spark = new SparkContext(host, "Sort",
      SPARK_HOME, List(TARGET_JAR_BIGDATABENCH))
    val filename = args(1)
    val save_file = args(2)
    if (args.length > 3) splits = args(3).toInt
    val lines = spark.textFile(filename, splits)
    val data_map = lines.map(line => {
      (line, 1)
    })
    val result = data_map.sortByKey().map{line => line._1}
    result.saveAsTextFile(save_file)
    println("Result has been saved to: " + save_file)
  }

}