/*
* Naive Bayes workload for BigDataBench
*/

package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer

import collection.mutable.HashMap

import BigDataBenchConf._

object NaiveBayes {

  type WCOUNT = HashMap[String, Int]
  //type CCOUNT = HashMap[String, Int]
  type MODEL = Pair[String, Pair[WCOUNT, Int]]

  def train(line: String): MODEL = {
    val _wcount = new WCOUNT() // [word, count]

    val tokenizer = line.split(" ")
    val category = tokenizer(0)

    for (i <- 1 until tokenizer.length) {
      val word = tokenizer(i)
      _wcount.put(word, 1)
    }

    (category, (_wcount, 1))
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: NaiveBayes <master> <data_file> " +
        "<save_file> [<slices>]")
      System.exit(1)
    }

    val host = args(0)
    val spark = new SparkContext(host, "NaiveBayes",
      SPARK_HOME, List(TARGET_JAR_BIGDATABENCH), null, null)
    val filename = args(1)
    val save_path = args(2)
    val slices = if (args.length > 3) args(3).toInt else 1
    println("Loading data, please wait...")
    val lines = spark.textFile(filename, slices)

    // train every line and return a RDD of classifier
    val models_rdd = lines.map { line => train(line) }

    // merge all classifier
    println("Reducing...")
    //val wcount = new WCOUNT
    //val ccount = new CCOUNT

    // this may need to be replaced with rdd.reduce
    val models_rk = models_rdd.reduceByKey(
      (m1, m2) => {
        val wcount = m1._1
        var ccount = m1._2
        for (word <- m2._1.keySet) {
          wcount.put(word, 1 + wcount.get(word).getOrElse(0))
        }
        ccount += m2._2
        (wcount, ccount)
      })

    // calculate the possibility
    val models_p = models_rk.map(model => {
      val _pos = new HashMap[String, Double]

      for (word <- model._2._1.keySet) {
        _pos.put(word, (model._2._1.get(word).get + 0.0) / model._2._2)
      }

      (model._1, _pos)
    })

    models_p.saveAsTextFile(save_path)
    println("Model has been saved to: " + save_path)

    System.exit(0)
  }
}