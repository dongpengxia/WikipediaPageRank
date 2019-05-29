import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.matching.Regex

//Gets titles of articles from Wikipedia that are about people

object FindPeople {
  //main method
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Find People")
    val sc = new SparkContext(sparkConf)

    //input comes from one file due to coalesce in previous program
    //input file, one article per line: title tab text
    val input = sc.textFile("/Documents/WikipediaArticles/part-00000")

    //previous output did not have parentheses around it, filter for articles about people
    val articles = input.map(l => {
      val pair = l.split("\t", 2)
      (pair(0), pair(1))
    }).filter(article => isPerson(article._2)).map(article => article._1)

    //use coalesce to save into one file
    articles.coalesce(1, true).saveAsTextFile("/Downloads/people")

    //terminate spark context
    sc.stop()
  }

  //return true if article text is about a person, false otherwise
  def isPerson(text: String): Boolean = {
    val doubleBrackets = "\\[\\[.*?\\]\\]".r

    //search for a category of type [[category:...people...]]
    val categoriesPeople = doubleBrackets.findAllIn(text).filter(link => {
      link.toLowerCase.contains("category:") && link.toLowerCase.contains("people")
    })

    return categoriesPeople.nonEmpty
  }
}