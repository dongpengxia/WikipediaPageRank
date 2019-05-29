import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//PeopleRank applies the PageRank algorithm to Wikipedia Articles about people using outlinks lists for each article.
//Each article starts with an initial PageRank of 1, and has its PageRank computed iteratively for numIteration times.
//Only articles about people are included.

object PeopleRank {
  
  def main(args: Array[String]) {

    //spark setup
    val sparkConf = new SparkConf().setAppName("People Rank").set("spark.executor.memory", "3g")
    val sc = new SparkContext(sparkConf)

    //input comes from one file due to coalesce in previous program
    val inputLinks = sc.textFile("/Documents/Wikipedia/outlinks/part-00000")

    //create set of people names to help filter out outlinks and articles that point outside dataset of people
    val peoples = sc.textFile("/Documents/Wikipedia/people/part-00000").collect.toSet

    //load RDD of (page title, outlinks) pairs
    val outlinks = inputLinks.map(l => { val pair = l.stripPrefix("(").stripSuffix(")").split(",", 2)
      (pair(0), pair(1)) }).filter(article => peoples.contains(article._1)).mapValues(outlinks => { outlinks.split("\t")
      .map(l => l.stripPrefix("[").stripPrefix("[").stripSuffix("]").stripSuffix("]"))
      .filter(link => peoples.contains(link)) }).persist()

    //load RDD of (page title, rank (double)) pairs
    var ranks = outlinks.mapValues(l => (1.0))

    //PeopleRank calculations
    val numIterations = 20
    for (i <- 0 to numIterations) {
      val contribs = outlinks.join(ranks).flatMap {
        case (title, (links, rank)) => links.map(dest => (dest, rank/links.length))
      }
      ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85 * _)
    }

    //save peopleranks, sort in descending order
    ranks.coalesce(1, true).sortBy(_._2, false).map(r => "[[" + r._1 + "]]\t" + r._2).saveAsTextFile("/Downloads/peoplerank")

    //terminate spark context
    sc.stop()
  }
}