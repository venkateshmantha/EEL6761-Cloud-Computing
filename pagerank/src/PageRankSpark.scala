import java.nio.file.{StandardOpenOption, Paths, Files}

import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object PagerankSpark {
  
  def writeToFile(r: String, path: String) = {
    print(r + "\n")
    Files.write(Paths.get(path), (r + "\n").getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }

  def printTopPages(wex_data: String, master_url: String, top_pages_cnt: Int, iterations: Int, path: String, sc: SparkContext) {

    var t1 = System.currentTimeMillis()
    val wiki: RDD[String] = sc.textFile(wex_data, 1)
    Files.deleteIfExists(Paths.get(path))

    case class Article(val id: Long, val title: String, val xml: String)

    val articles = wiki.map(_.split('\t')).
      filter(line => line.length > 1).
         map(line => new Article(line(0).trim.toLong, line(1).trim, line(3).trim)).cache()

    writeToFile("Total articles " + articles.count(), path)

    val pattern = "<target>.+?<\\/target>".r

    val links: RDD[(String, Iterable[String])] = articles.flatMap { a =>

      pattern.findAllIn(a.xml).map { link =>
        val dstId = link.replace("<target>", "").replace("</target>", "")
        (a.title, dstId)
      }
    }.distinct().groupByKey().cache()

    writeToFile("Total links count " + links.count(), path)

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iterations) {

      println("Iterations count " + i)
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      println("Ranks " + ranks.count())
    }

    var output1 = ranks.sortBy(_._2, false).collect()

    output1.take(top_pages_cnt).foreach(tup => {
      writeToFile(tup._1 + " has rank: " + tup._2, path)
    })
    writeToFile("Total time taken => " + (System.currentTimeMillis() - t1 ), path)
  }

  
}