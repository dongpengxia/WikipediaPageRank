import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//PeopleOnlyArticles filters the PageRanked Wikipedia articles for articles about people.

object PeopleOnly {
  def main(args: Array[String]) {

    //spark setup
    val sparkConf = new SparkConf().setAppName("People Only").set("spark.executor.memory", "3g")
    val sc = new SparkContext(sparkConf)

    //create set of people names to help filter out articles that are not about people
    val peoples = sc.textFile("/Documents/Wikipedia/people/part-00000").collect.toSet

    //comes from one file due to coalesce in previous program
    val original = sc.textFile("/Documents/Wikipedia/bigPageRank/part-00000")

    val ranks = original.map(l => { val pair = l.split("\t", 2)
                    (pair(0), pair(1).toDouble) }).filter(article => peoples.contains(article._1.stripPrefix("[").stripPrefix("[").stripSuffix("]").stripSuffix("]")))

    //save people-ranks, presorted from pageRank
    ranks.coalesce(1, true).saveAsTextFile("/Downloads/pageRankPeopleOnly")

    //terminate spark context
    sc.stop()
  }
}