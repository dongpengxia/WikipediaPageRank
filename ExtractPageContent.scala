import java.io.File
import java.io.PrintWriter
import scala.collection.mutable.StringBuilder
import scala.io.Source

//Processes a file line by line and outputs a new file where each line is a <page>...</page> from the
//original file. The "..." is any text between <page> and </page> in the original file. The number of pages
//in the original file is then output to screen. The program takes between 10-20 minutes to complete on the
//entire English Wikipedia (65 GB).

//Notes about the original Wikipedia file:
//There is at most one "<page>" in a line
//There is at most one "</page>" in a line
//There can not be a "<page>" and a "</page>" in one line
//There are 18,136,268 <page>s and 18,136,268 </page>s in the file
//The file has 1,007,328,662 lines (use wc -l to find the # of lines in a file)
//These results were found by empirical testing

object PreProc {

  def main(args: Array[String]) {

    //location of original Wikipedia file
    val inputFile = "/Documents/enwiki-latest-pages-articles-multistream.xml"

    //location of output file to contain preprocessed Wikipedia (to be created and written to by program)
    val outputFile = new PrintWriter(new File("/Downloads/EnglishWikipediaOnePagePerLine"))

    //outputLine will hold one <page>...</page>
    var outputLine = new StringBuilder

    //The first pair of regex patterns is used to quickly detect when a page start or end exists in a line.
    //The second pair of regex patterns is slower, and is used to extract parts of the line that end in </page>
    //or start with <page>. The second pair of regex patterns is applied only when the first pair of regex
    //patterns successfully finds a match in a line.

    //regex patterns for start of page (<page>) and end of page (</page>)
    val pageStartPattern = "<page>".r
    val pageEndPattern = "</page>".r

    //regex patterns for start of page (<page>) to end of line and start of line to end of page (</page>)
    val pageStartExtract = "<page>.*".r
    val pageEndExtract = ".*</page>".r

    //inPage is true if currently reading inside a <page>...</page>, false otherwise
    var inPage = false

    //number of "<page>"s found
    var pageStartCount = 0
    //number of "</page>"s found
    var pageEndCount = 0

    //search is used to store the regex results when applied to each line
    var search: String = null

    //read file one line at a time
    for (inputLine <- Source.fromFile(inputFile).getLines) {

      //if currently reading inside a <page>...</page>
      if (inPage) {
        //search for end of page (</page>) in line
        search = pageEndPattern.findFirstIn(inputLine).mkString

        //if search did not find end of page (</page>) in line
        //then add line to current page for output later
        if(search.isEmpty) {
          outputLine.append(inputLine.trim)
        }
        //if search did find end of page (</page>) in line
        //then add words up to </page> to outputLine,
        //write page to output file, and update variables
        else {
          outputLine.append(pageEndExtract.findFirstIn(inputLine).mkString.trim)
          outputFile.write(outputLine.toString)
          outputFile.write("\n")
          outputLine.clear()
          inPage = false
          pageEndCount += 1
        }
      }
      //if not currently reading inside a <page>...</page>
      else {
        //search for start of page (<page>) in line
        search = pageStartPattern.findFirstIn(inputLine).mkString

        //if search found start of page (<page>) in line
        //then add line contents starting at <page> to outputLine
        //and update variables
        if(!search.isEmpty) {
          outputLine.append(pageStartExtract.findFirstIn(inputLine).mkString.trim)
          inPage = true
          pageStartCount += 1
        }
      }
    }

    if(pageStartCount == pageEndCount) {
      println(s"The file has $pageStartCount pages.")
    }
    else {
      println("ERROR: Number of <page> and </page> do not equal.")
      println(s"Number of <page>: $pageStartCount")
      println(s"Number of </page>: $pageEndCount")
    }
    outputFile.close
  }
}