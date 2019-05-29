import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//PageRank applies the PageRank algorithm to Wikipedia Articles using outlinks lists for each article.
//Each article starts with an initial PageRank of 1, and has its PageRank computed iteratively for numIteration times.
//Observation from small dataset: With one iteration, set filtering reduces runtime from 249.4 seconds to 82.2 seconds.

object PageRank {
  def main(args: Array[String]) {

    //spark setup
    val sparkConf = new SparkConf().setAppName("PageRank").set("spark.executor.memory", "3g")
    val sc = new SparkContext(sparkConf)

    //input comes from one file due to coalesce in previous program
    val inputLinks = sc.textFile("/Downloads/outlinks/part-00000")

    //create set of article titles to help filter out outlinks that point outside dataset
    //(in the small dataset some outlinks go to pages that are not in the dataset)
    val titles = inputLinks.map(l => (l.stripPrefix("(").stripSuffix(")").split(",")(0))).collect.toSet

    //load RDD of (page title, outlinks) pairs
    val outlinks = inputLinks.map(l => { val pair = l.stripPrefix("(").stripSuffix(")").split(",", 2)
                                      (pair(0), pair(1)) }).mapValues(outlinks => { outlinks.split("\t")
                                      .map(l => l.stripPrefix("[").stripPrefix("[").stripSuffix("]").stripSuffix("]"))
                                      .filter(link => titles.contains(link)) }).persist()

    //load RDD of (page title, rank (double)) pairs
    var ranks = outlinks.mapValues(l => (1.0))

    //PageRank calculations
    val numIterations = 20
    for (i <- 0 to numIterations) {
      val contribs = outlinks.join(ranks).flatMap {
        case (title, (links, rank)) => links.map(dest => (dest, rank/links.length))
      }
      ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85 * _)
    }

    //save pageranks in compressed format, sort in descending order
    ranks.coalesce(1, false).sortBy(_._2, false).map(r => "[[" + r._1 + "]]\t" + r._2).saveAsTextFile("/Downloads/pagerank", classOf[GzipCodec])

    //terminate spark context
    sc.stop()
  }
}