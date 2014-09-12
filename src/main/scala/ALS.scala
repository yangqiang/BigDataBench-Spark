/*
 * ALS workload for BigDataBench
 */
package cn.ac.ict.bigdatabench


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating


object ALS {
  val MLLibALS = org.apache.spark.mllib.recommendation.ALS

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: ALS <data_file> <rank> <iterations>" +
        " [<slices>]")
      System.exit(1)
    }

    var splits = 2
    val conf = new SparkConf().setAppName("BigDataBench ALS")
    val sc = new SparkContext(conf)

    val filename = args(0)
    val rank = args(1).toInt
    val iterations = args(2).toInt
    if (args.length > 3) splits = args(3).toInt

    println("Start ALS...")
    // Load and parse the data
    val data = sc.textFile(filename, splits)
    val parsedData = data.map(s => {
      val parts = s.split(",")
      Rating(parts(0).toInt, parts(1).toInt, parts(2).toDouble)
    })

    val result = MLLibALS.train(parsedData, rank, iterations)

    println("OK. Finished.")
  }
}