/*
 * Grep workload for BigDataBench
 */
package cn.ac.ict.bigdatabench


import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Grep {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: Grep <data_file> <keyword> <save_file>" +
        " [<slices>]")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("BigDataBench Grep")
    val spark = new SparkContext(conf)

    var splits = 2
    val filename = args(0)
    val keyword = args(1)
    val save_file = args(2)
    if (args.length > 3) splits = args(3).toInt

    val lines = spark.textFile(filename, splits)
    val result = lines.filter(line => line.contains(keyword))

    result.saveAsTextFile(save_file)
    println("Result has been saved to: " + save_file)
  }
}