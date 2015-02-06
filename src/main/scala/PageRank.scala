/**
 * This program is based on examples of spark-0.9.1
 * The original source file is: org.apache.spark.examples.SparkPageRank
 */

package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import BigDataBenchConf._
import org.apache.spark.storage.StorageLevel
/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL neighbor URL
 * URL neighbor URL
 * URL neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object PageRank {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: PageRank <master> <file> <number_of_iterations> <save_path> [<slices>]")
      System.exit(1)
    }
    var iters = args(2).toInt
    var slices = 1
    val save_path = args(3)
    if (args.length > 4) slices = args(4).toInt
    val ctx = new SparkContext(args(0), "PageRank",
      SPARK_HOME, Seq(TARGET_JAR_BIGDATABENCH))

    // load data
    val lines = ctx.textFile(args(1), slices)

    // directed edges: (from, (to1, to2, to3))
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    println(links.count.toString + " links loaded.")
    // rank values are initialised with 1.0
    var ranks = links.mapValues(v => 1.0).persist(StorageLevel.MEMORY_AND_DISK)

    for (i <- 1 to iters) {
      // calculate contribution to desti-urls
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }.persist(StorageLevel.MEMORY_AND_DISK)
      // This may lead to points' miss if a page have no link-in
      // add all contribs together, then calculate new ranks
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

    }

    // show results
    //val output = ranks.collect()
    //output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    println("Result saved to: " + save_path)
    ranks.saveAsTextFile(save_path)

    System.exit(0)
  }
}