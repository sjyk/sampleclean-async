package sampleclean.clean.featurize
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{SchemaRDD, Row}

/**
 * This is a tokenizer super-class.
 */
abstract class Tokenizer{
  
  def tokenize(row: Row, cols:List[Int]): List[String] = {

      var stringA = ""
      for (col <- cols){
        stringA = stringA + " " + row(col).asInstanceOf[String]
      }
      return tokenSet(stringA)
    }
  
  def tokenSet(text: String): List[String]
}

object Tokenizer {
/**
 * This class tokenizes a string based on user-specified delimiters.
 * @param delimiters string of delimiters to be used for splitting. Accepts regex expressions.
 */
case class DelimiterTokenizer(delimiters: String = ".,?!\t ") extends Tokenizer {

  def tokenSet(str: String) = {
    val st = new StringTokenizer(str, delimiters)
    val tokens = new ArrayBuffer[String]
    while (st.hasMoreTokens()) {
      tokens += st.nextToken()
    }
    tokens.toList
  }
}

/**
 * This class tokenizes a string based on words.
 */
case class WordTokenizer() extends Tokenizer {
  def tokenSet(str: String) = str.split("\\W+").toList.filter(_!="")
}

/**
 * This class tokenizes a string based on white spaces.
 */
case class WhiteSpaceTokenizer() extends Tokenizer {
  def tokenSet(str: String) = str.split("\\s+").toList.filter(_!="")
}

/**
 * This class tokenizes a string based on grams.
 * @param gramSize size of gram.
 */
case class GramTokenizer(gramSize: Int) extends Tokenizer {
  def tokenSet(str: String) =  str.sliding(gramSize).toList
}

/**
 * This class tokenizes a string based on white space punctuation.
 */
case class WhiteSpacePunctuationTokenizer() extends Tokenizer {
  def tokenSet(str: String) =  str.trim.split("([.,!?:;'\"-]|\\s)+").toList
}
}