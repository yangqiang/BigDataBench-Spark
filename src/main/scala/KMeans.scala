/*
 * KMeans workload for BigDataBench
 */
package cn.ac.ict.bigdatabench


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors

object KMeans {
  val MLLibKMeans = org.apache.spark.mllib.clustering.KMeans

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      //System.err.println("Usage: KMeans <master> <data_file> <k> <iterations> <save_path>" +
      System.err.println("Usage: KMeans <data_file> <k> <iterations>" +
        " [<slices>]")
      System.exit(1)
    }

    var splits = 2
    val conf = new SparkConf().setAppName("BigDataBench KMeans")
    val sc = new SparkContext(conf)

    val filename = args(0)
    val k = args(1).toInt
    val iterations = args(2).toInt
    // val save_file = args(4)
    //if (args.length > 5) splits = args(5).toInt
    if (args.length > 3) splits = args(3).toInt

    println("Start KMeans training...")
    // Load and parse the data
    val data = sc.textFile(filename, splits)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    val clusters = MLLibKMeans.train(parsedData, k, iterations)
    //val clusters = org.apache.spark.mllib.clustering.KMeans.train(parsedData, k, iterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("OK. Within Set Sum of Squared Errors = " + WSSSE)
  }
}
