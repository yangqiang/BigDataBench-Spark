/*
 * Connected Component workload for BigDataBench
 */
package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._
import org.apache.spark.SparkConf


object ConnectedComponent {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: ConnectedComponent <data_file>" +
        " [<slices>]")
      System.exit(1)
    }

    var splits = 2
    val conf = new SparkConf().setAppName("BigDataBench ConnectedComponent")
    val spark = new SparkContext(conf)
    val filename = args(0)

    if (args.length > 1) splits = args(1).toInt
    val graph = GraphLoader.edgeListFile(spark, filename, false, splits)
    val cc = graph.connectedComponents()

    System.exit(0)
  }

}
