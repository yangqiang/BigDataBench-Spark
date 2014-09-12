/*
 * KMeans workload for BigDataBench
 */
package cn.ac.ict.bigdatabench


import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object BigDataBenchKMeans {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      //System.err.println("Usage: KMeans <master> <data_file> <k> <iterations> <save_path>" +
      System.err.println("Usage: KMeans <master> <data_file> <k> <iterations>" +
        " [<slices>]")
      System.exit(1)
    }

    val host = args(0)
    var splits = 1

    /*val spark = new SparkContext(host, "KMeans",
      SPARK_HOME, List(TARGET_JAR_BIGDATABENCH))*/
    val conf = new SparkConf().setAppName("BigDataBench KMeans")
    val sc = new SparkContext(conf)

    val filename = args(1)
    val k = args(2).toInt
    val iterations = args(3).toInt
    // val save_file = args(4)
    //if (args.length > 5) splits = args(5).toInt
    if (args.length > 4) splits = args(4).toInt


    println("Start KMeans training...")

    // Load and parse the data
    val data = sc.textFile(filename, splits)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

    val clusters = KMeans.train(parsedData, k, iterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("OK. Within Set Sum of Squared Errors = " + WSSSE)

  }

}