import java.nio.file.{StandardOpenOption, Paths, Files}
import java.util.Scanner
import scala.math.Ordering
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup
import scala.io.Source



object Main {

  var wex_data = "data/freebase-wex-2009-01-12-articles.tsv"
  var master_url = "local"
  val top_pages_cnt = 100
  val iterations = 5

  val graphx_result = "graphx_result.log"
  val spark_result = "spark_result.log"
  val top100_pages_result = "top100_pages_result.log"

  def main(args: Array[String]) {
    
    
    def writeToFile(r: String, path: String) = {
    println(r)
    Files.write(Paths.get(path), r.getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }

    //data location
    if (args.length >= 1) {
      wex_data = args(0).toString.trim
    }

    //master location
    if (args.length >= 2) {
      master_url = args(1).toString.trim
    }

    println("Top pages count : " + top_pages_cnt)

    val sparkConf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(sparkConf)

    var scan = new Scanner(System.in)
    var loop = true
    var list: Graph[(Double, String), Double] = null

    while (loop) {
      Thread.sleep(500)
      println("Enter: \n\t1 - Pagerank using GraphX\n\t2 - Pagerank using Spark \n\t3 - Top 100 University List")
      println("Running..")
      var num = scan.nextLine()
      var opt = 1
      if (num.length > 0) {
        opt = num.toInt
      }
      println("Read " + opt)

      if (opt == 1) {
        list = PageRankGraphX.printTopPages(wex_data, master_url, top_pages_cnt, iterations, graphx_result, sc)
      }

      if (opt == 2) {
        PagerankSpark.printTopPages(wex_data, master_url, top_pages_cnt, iterations, spark_result, sc)
      }

      if (opt == 3) {
        if (list == null) {
          list = PageRankGraphX.printTopPages(wex_data, master_url, top_pages_cnt, iterations, graphx_result, sc)
        }
        Files.deleteIfExists(Paths.get(top100_pages_result))

        writeToFile("\n", top100_pages_result)
        val l = Source.fromURL(getClass.getResource("/univ_list.txt")).getLines().toList

        var p = list.vertices.top(list.triplets.count().toInt) {
          Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
        }.filter{ x => 
          l contains (x._2._2)
        }.take(100).foreach(x => writeToFile("\n" + x._2._2 + " has rank : " + x._2._1, top100_pages_result))
      }

      if (opt == -1) {
        System.exit(0)
        sc.stop()
      }
    }
  }

  

  
}