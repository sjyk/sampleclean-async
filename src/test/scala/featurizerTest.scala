package dedupTesting

import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._
import sampleclean.clean.featurize._
import sampleclean.clean.featurize.Tokenizer._


class featurizerTest extends FunSuite with Serializable {

  test("Tokenizer"){
    val str = """ a`b~c!d@e#f$g%h^i&j*k(l)m_n-o+p=q[r}s\tñu|v;w:x"y'z/aa?bb>cc<dd,ee.ff gg """
    val emptyStr = ""
    var tok: Tokenizer = null

    // Delimiter Tokenizer
    tok = new DelimiterTokenizer(":")
    assert(tok.tokenSet(emptyStr) == Seq())
    assert(tok.tokenSet(str) == Seq(""" a`b~c!d@e#f$g%h^i&j*k(l)m_n-o+p=q[r}s\tñu|v;w""","""x"y'z/aa?bb>cc<dd,ee.ff gg """))

    // Word Tokenizer
    // does not split by _
    tok = new WordTokenizer
    assert(tok.tokenSet(emptyStr) == Seq())
    assert(tok.tokenSet(str) == {
      Seq("a","b","c","d","e","f","g","h","i","j","k","l","m_n") ++
        Seq("o","p","q","r","s","t","u","v","w","x","y","z","aa","bb","cc","dd","ee","ff","gg")
    })

    // Null Tokenizer
    tok = new NullTokenizer
    assert(tok.tokenSet(emptyStr) == Seq())
    assert(tok.tokenSet(str) == Seq(""" a`b~c!d@e#f$g%h^i&j*k(l)m_n-o+p=q[r}s\tñu|v;w:x"y'z/aa?bb>cc<dd,ee.ff gg """))

    // WhiteSpace Tokenizer
    tok = new WhiteSpaceTokenizer
    assert(tok.tokenSet(emptyStr) == Seq())
    assert(tok.tokenSet(str) == Seq("""a`b~c!d@e#f$g%h^i&j*k(l)m_n-o+p=q[r}s\tñu|v;w:x"y'z/aa?bb>cc<dd,ee.ff""", "gg"))

    // Gram Tokenizer
    tok = new GramTokenizer(71)
    assert(tok.tokenSet(emptyStr) == Seq())
    assert(tok.tokenSet(str) == {
      Seq(""" a`b~c!d@e#f$g%h^i&j*k(l)m_n-o+p=q[r}s\tñu|v;w:x"y'z/aa?bb>cc<dd,ee.ff """) ++
        Seq("""a`b~c!d@e#f$g%h^i&j*k(l)m_n-o+p=q[r}s\tñu|v;w:x"y'z/aa?bb>cc<dd,ee.ff g""") ++
        Seq("""`b~c!d@e#f$g%h^i&j*k(l)m_n-o+p=q[r}s\tñu|v;w:x"y'z/aa?bb>cc<dd,ee.ff gg""") ++
        Seq("""b~c!d@e#f$g%h^i&j*k(l)m_n-o+p=q[r}s\tñu|v;w:x"y'z/aa?bb>cc<dd,ee.ff gg """)
    })

    // WhiteSpacePunctuation Tokenizer
    // does not split by ( ) [ ] { } / *
    tok = new WhiteSpacePunctuationTokenizer
    assert(tok.tokenSet(emptyStr) == Seq())
    assert(tok.tokenSet(str) == {
      Seq("a`b~c","d@e#f$g%h^i&j*k(l)m_n") ++
        Seq("""o+p=q[r}s\tñu|v""","w","x","y","""z/aa""","bb>cc<dd","ee","ff","gg")
    })

    // Tokenize function
    tok = new DelimiterTokenizer(" ")
    assert(tok.tokenize(Row(), List()) == Seq())
    assert(tok.tokenize(Row("a","Juan Sanchez","94720 Berkeley","d"), List(1,2)) == Seq("Juan","Sanchez", "94720", "Berkeley"))
    assert(tok.tokenize(Row("a","Juan Sanchez","94720","d"), List(1,2)) == Seq("Juan","Sanchez", "94720"))

    tok = new DelimiterTokenizer(",")
    assert(tok.tokenize(Row("a","b,c","d"), List(1,2)) == Seq("b","c","d"))
  }

  test("similarity featurizer"){
    // Test SampleClean similarity implementations
    var metrics = List("JaccardSimilarity","DiceSimilarity","CosineSimilarity","OverlapSimilarity")


    def distances(seq1: Seq[String],seq2: Seq[String]): Seq[Double] = {

      val inter = seq1.intersect(seq2).size.toDouble

      if(math.sqrt(seq1.size * seq2.size) == 0 ||
        (seq1.size + seq2.size - inter) == 0 ||
        (seq1.size + seq2.size) == 0) 0.0

        val cosD = {
          if(math.sqrt(seq1.size * seq2.size) == 0.0) 0.0
          else inter / math.sqrt(seq1.size * seq2.size)
        }
        val jaccD = {
          if (seq1.size + seq2.size - inter == 0) 0.0
          else inter / (seq1.size + seq2.size - inter)
        }
        val diceD = {
          if ((seq1.size + seq2.size) == 0) 0.0
          else 2 * inter / (seq1.size + seq2.size)
        }
        val overlap = inter

        Seq(jaccD,diceD,cosD,overlap)




    }

    val context = List("col1","col2","col3","col4")
    var colNames: List[String] = null
    var featurizer: Featurizer = null

    // Empty case
    colNames = List()
    featurizer = new SimilarityFeaturizer(colNames,context, metrics)
    var row1 = Row()
    var row2 = Row()

    var feat = featurizer.featurize(Set(row1,row2))
    assert((feat._1, feat._2.toSeq) == (Set(row1,row2), Seq(0.0,0.0,0.0,0.0)))

    // Equal rows
    colNames = context
    row1 = Row("a","b","c","d")
    row2 = Row("a","b","c","d")

    featurizer = new SimilarityFeaturizer(colNames,context, metrics)
    feat = featurizer.featurize(Set(row1,row2))
    assert((feat._1, feat._2.toSeq) == (Set(row1,row2), Seq(1.0,1.0,1.0,4.0)))

    // Single attribute
    colNames = List("col1")
    row1 = Row("a b c d","b c","c","d")
    row2 = Row("a c","b","c","d")

    featurizer = new SimilarityFeaturizer(colNames,context, metrics)
    feat = featurizer.featurize(Set(row1,row2))
    assert((feat._1, feat._2.toSeq) == (Set(row1,row2), distances(Seq("a","b","c","d"),Seq("a","c"))))

    // Multiple attribute
    colNames = List("col1","col2")
    featurizer = new SimilarityFeaturizer(colNames,context, metrics)
    feat = featurizer.featurize(Set(row1,row2))
    assert((feat._1, feat._2.toSeq) == (Set(row1,row2), distances(Seq("a","b","c","d","b","c"),Seq("a","c","b"))))

    // featurize
    colNames = List("col1")
    featurizer = new SimilarityFeaturizer(colNames,context, metrics)
    feat = featurizer.featurize(Set(Row("a "),Row(" a")))
    assert((feat._1, feat._2.toSeq) == (Set(Row("a "),Row(" a")), Seq(1.0,1.0,1.0,1.0)))

    // TODO include weighted similarity joins?
    // TODO EditDistance


   /* //blocking featurizer
    val thresh = 0.75
    val wTok = Tokenizer.WordTokenizer()
    val blockFeat = new WeightedJaccardSimilarity(List(""), List(), wTok,thresh)

    val tRow1 = wTok.tokenize(row1,List(0,1,2,3))
    val tRow2 = wTok.tokenize(row2,List(0,1,2))

    assert(blockFeat.similar(tRow1, tRow2,thresh, collection.Map[String, Double]()))*/
  }

  test("featurizer"){
    val context = List("col1","col2")
    val colNames = List("col1","col2")
    val metrics = List("JaccardSimilarity")
    val featurizer = new SimilarityFeaturizer(colNames,context, metrics)

    featurizer.setContext(List("col2","col1"))
    assert(featurizer.cols == List(1,0))
  }

  test("ensemble featurizer"){
    case class testFeat(cols:List[Int], featurizers:List[Featurizer])
                extends EnsembleFeaturizer(cols, featurizers)

    val context = List("col1","col2")
    val colNames = List("col1","col2")
    var metrics = List("JaccardSimilarity")

    val feat1 = new SimilarityFeaturizer(colNames,context, metrics)
    metrics = List("OverlapSimilarity")
    val feat2 = new SimilarityFeaturizer(colNames,context, metrics)

    val ensemble = new testFeat(feat1.cols, List(feat1,feat2))

    val row1 = Row("a","b","c","d")
    val row2 = Row("a","b","c","d")

    val ensFeat = ensemble.featurize(Set(row1,row2))
    assert((ensFeat._1, ensFeat._2.toSeq) == (Set(row1,row2),Seq(1.0,2.0)))

  }

  test("annotated similarity featurizer"){
    val context = List("col1","col2","col3","col4","col5")
    var colNames = context
    val tok = new WhiteSpaceTokenizer()

    var sim: AnnotatedSimilarityFeaturizer = null
    var thresh: Double = 0.0
    var seq1: Seq[String] = null
    var seq2: Seq[String] = null
    var weights: Map[String,Double] = Map[String,Double]()
    weights = Map[String,Double]("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)

    // Weighted Jaccard
    thresh = 3.0 / 7
    sim = new WeightedJaccardSimilarity(colNames,context,tok,thresh)
    seq1 = Seq("a","b","c","a","c")
    seq2 = Seq("a","b","d","a","e")

    assert(sim.similar(seq1,seq2,thresh,Map[String,Double]()))
    assert(sim.getSimilarity(seq1,seq2,Map[String,Double]()) == thresh)
    assert(sim.getSimilarity(seq1,seq2,weights) == 2.0 / 7)

    // Weighted Overlap
    thresh = 3.0
    sim = new WeightedOverlapSimilarity(colNames,context,tok,thresh)

    assert(sim.similar(seq1,seq2,thresh,Map[String,Double]()))
    assert(sim.getSimilarity(seq1,seq2,Map[String,Double]()) == thresh)
    assert(sim.getSimilarity(seq1,seq2,weights) == 4.0)

    // Weighted Dice
    thresh = 0.6
    sim = new WeightedDiceSimilarity(colNames,context,tok,thresh)

    assert(sim.similar(seq1,seq2,thresh,Map[String,Double]()))
    assert(sim.getSimilarity(seq1,seq2,Map[String,Double]()) == thresh)
    assert(sim.getSimilarity(seq1,seq2,weights) == 4.0 / 9)

    // Weighted Cosine
    thresh = 0.6
    sim = new WeightedCosineSimilarity(colNames,context,tok,thresh)

    assert(sim.similar(seq1,seq2,thresh,Map[String,Double]()))
    assert(sim.getSimilarity(seq1,seq2,Map[String,Double]()) == thresh)
    assert(sim.getSimilarity(seq1,seq2,weights) == 1.0 / math.sqrt(5))


    // Edit
    thresh = 2.0
    sim = new EditBlocking(colNames,context,tok,thresh)

    assert(sim.similar(seq1,seq2,thresh,Map[String,Double]()))
    assert(sim.getSimilarity(seq1,seq2,Map[String,Double]()) == thresh)
    assert(sim.getSimilarity(seq1,seq2,weights) == 2.0)


    // featurize
    val row1 = Row("a","b","c","a","c")
    val row2 = Row("a","b","d","a","e")

    colNames = context
    sim = new WeightedOverlapSimilarity(colNames,context,tok,4.0)
    assert(sim.featurize(Set(row1,row2),weights)._2.toSeq == Seq(1.0))

    colNames = List("col1","col2","col3")
    sim = new WeightedOverlapSimilarity(colNames,context,tok,3.0)
    assert(sim.featurize(Set(row1,row2),weights)._2.toSeq == Seq(1.0))

    sim = new WeightedOverlapSimilarity(colNames,context,tok,4.0)
    assert(sim.featurize(Set(row1,row2),weights)._2.toSeq == Seq(0.0))


  }

  // TODO Learning Similarity Featurizer

  test("Blocking with tokenizer and featurizer"){

  }

}
