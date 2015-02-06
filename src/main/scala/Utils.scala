package cn.ac.ict.bigdatabench

import java.util.Calendar

/**
 * Some useful methods for graph related workloads
 */
object Utils {
  /**
   * cut short a URL: split the url with '/' and keep only the last part
   */
  def shorten_link(link: String):String = {
    val parts = link.split("/")
    parts.last
  }
  def current_time():String = {
    Calendar.getInstance().getTime().toString() + " "
  }
  def log_print(line: String) = {
    println(current_time + line)
  }
  def main(args: Array[String]) {
    val url = "http://a.very.very.long.url/1000000";
    println("Original URL: " + url)
    println("Short URL: " + shorten_link(url))
    System.exit(0)
  }
}