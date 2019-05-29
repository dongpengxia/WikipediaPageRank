import scala.util.matching.Regex
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//WikiArticles filters the Wikipedia pages so that only articles are left. Disambiguations,
//redirects, and stubs are all removed.

object WikiArticle {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Wiki WordCount")
    val sc = new SparkContext(sparkConf)

    //input file
    val txt = sc.textFile("/Documents/EnglishWikipediaOnePagePerLine")

    //get title of page and text in page
    val getTitleAndText = txt.map { l =>
      val line = XML.loadString(l)
      val title = (line \ "title").text
      val text = (line \\ "text").text
      (title, text)
    }

    //filter RDD so that only articles remain
    val articles = getTitleAndText.filter { r => isArticle(r._2.toLowerCase) }

    //tab-separated values, use coalesce to save as one file later
    val output = articles.map{ article => article._1 + "\t" + article._2 }.coalesce(1, true)

    //end of transformations, start of actions

    //output # of articles
    println("There are " + output.count + " articles.")

    //output file
    output.saveAsTextFile("/Downloads/Articles")

    //terminate spark context
    sc.stop()
  }

  def isArticle(text: String): Boolean = {

    //avoid using * as it will slow down the program on a large dataset
    //return false immediately if text is not an article to save time

    //regex patterns for disambiguation, handles {{disambiguation}} and {{disambig}}
    val disambiguation = "\\{\\{disambig".r
    if (disambiguation.findFirstIn(text).nonEmpty)
      return false

    //regex pattern for stub -stub}}
    val stub = "-stub\\}\\}".r
    if(stub.findFirstIn(text).nonEmpty)
      return false

    //regex pattern for redirect #redirect
    val redirect = "#redirect".r
    if(redirect.findFirstIn(text).nonEmpty)
      return false

    //return true if text is an article
    return true
  }
}