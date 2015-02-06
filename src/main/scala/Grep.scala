/*
* Grep workload for BigDataBench
*/
package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import BigDataBenchConf._

object Grep {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: Grep <master> <data_file> <keyword> <save_file>" +
        " [<slices>]")
      System.exit(1)
    }

    val host = args(0)
    var splits = 1
    val spark = new SparkContext(host, "Grep",
      SPARK_HOME, List(TARGET_JAR_BIGDATABENCH))
    val filename = args(1)
    val keyword = args(2)
    val save_file = args(3)
    if (args.length > 4) splits = args(4).toInt
    val lines = spark.textFile(filename, splits)
    val result = lines.filter(line => line.contains(keyword))
    //println(result.collect.mkString("\n"))
    result.saveAsTextFile(save_file)
    println("Result has been saved to: " + save_file)
  }

}