
package dedupTesting

import org.apache.spark.sql.catalyst.expressions.Row
import sampleclean.clean.featurize.{WeightedJaccardBlocking, Tokenizer, SimilarityFeaturizer}
import org.scalatest.FunSuite


/**
 * Created by juanmanuelsanchez on 2/3/15.
 */
class FeaturizeTest extends FunSuite with Serializable {

  test("tokenizer"){
    val rowDel = Row("a::b:C")
    assert(Tokenizer.DelimiterTokenizer(":").tokenize(rowDel,List(0)) == List("a","b","C"))

    val rowWord = Row("aa b:C")
     assert(Tokenizer.WordTokenizer().tokenize(rowWord, List(0)) == List("aa","b","C"))

    val rowWhite = Row("  a:a  b, c ")
    assert(Tokenizer.WhiteSpaceTokenizer().tokenize(rowWhite,List(0)) == List("a:a","b,","c"))

    // TODO gram tokenizer
    val rowWhitePunc = Row(" a  b.c'd")
    assert(Tokenizer.WhiteSpacePunctuationTokenizer().tokenize(rowWhitePunc,List(0)) == List("a","b","c","d"))
  }

  test("featurize") {

    //similarity featurizer
    val simFeat = new SimilarityFeaturizer(List(0,1,2),List("CosineSimilarity", "JaccardSimilarity"))

    val row1 = Row("a","b","c","d")
    val row2 = Row("a","b","b")

    val inter = row1.intersect(row2).size.toDouble
    println(row1.intersect(row2))
    val cosD =  inter / math.sqrt(row1.size * row2.size)
    val jaccD = inter / (row1.size + row2.size - inter)

    val feat = simFeat.featurize(Set(row1,row2))
    assert((feat._1,feat._2.toSeq)== (Set(row1,row2), Array(cosD,jaccD).toSeq))

    //blocking featurizer
    val thresh = 0.75
    val wTok = Tokenizer.WordTokenizer()
    val blockFeat = new WeightedJaccardBlocking(List(0), wTok,thresh)

    val tRow1 = wTok.tokenize(row1,List(0,1,2,3))
    val tRow2 = wTok.tokenize(row2,List(0,1,2))

    assert(blockFeat.similar(tRow1, tRow2,thresh, collection.Map[String, Double]()))
  }

}
