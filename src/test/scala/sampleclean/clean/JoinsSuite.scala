package sampleclean.clean

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.FunSuite
import sampleclean.clean.deduplication.join.{BroadcastJoin, PassJoin, SimilarityJoin}
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._
import sampleclean.clean.featurize.Tokenizer.DelimiterTokenizer


class JoinsSuite extends FunSuite with LocalSCContext {

  test("broadcast self join un-weighted") {
    withSampleCleanContext { scc =>
      val sc = scc.getSparkContext()
      val context = List("record")
      val colNames = List("record")
      val tok = new DelimiterTokenizer(" ")
      val path = "./src/test/resources"
      var blocker: AnnotatedSimilarityFeaturizer = null
      var bJoin: SimilarityJoin = null
      var rowRDDLarge: RDD[Row] = null
      //rowRDDLarge = sc.textFile(path + "/dirtyJaccard100dups").map(r => Row.fromSeq(r.split(":").toSeq))

      // 100 duplicates with Jaccard similarity = 0.5
      rowRDDLarge = sc.textFile(path + "/dirtyJaccard100dups").map(Row(_))
      blocker = new WeightedJaccardSimilarity(colNames, context, tok, 0.51)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 0)
      blocker = new WeightedJaccardSimilarity(colNames, context, tok, 0.5)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)


      // 100 duplicates with Overlap similarity = 5
      rowRDDLarge = sc.textFile(path + "/dirtyOverlap100dups").map(Row(_))
      blocker = new WeightedOverlapSimilarity(colNames, context, tok, 6)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 0)
      blocker = new WeightedOverlapSimilarity(colNames, context, tok, 5)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)


      // 100 duplicates with Dice similarity = 0.8
      rowRDDLarge = sc.textFile(path + "/dirtyDice100dups").map(Row(_))
      blocker = new WeightedDiceSimilarity(colNames, context, tok, 0.81)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 0)
      blocker = new WeightedDiceSimilarity(colNames, context, tok, 0.8)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)


      // 100 duplicates with Cosine similarity = 0.5
      rowRDDLarge = sc.textFile(path + "/dirtyCosine100dups").map(Row(_))
      blocker = new WeightedCosineSimilarity(colNames, context, tok, 0.51)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 0)
      blocker = new WeightedCosineSimilarity(colNames, context, tok, 0.5)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)

    }

  }

  test("broadcast self join weighted"){
    withSampleCleanContext { scc =>
      val sc = scc.getSparkContext()
      val context = List("record")
      val colNames = List("record")
      val tok = new DelimiterTokenizer(" ")
      val path = "./src/test/resources"
      var blocker: AnnotatedSimilarityFeaturizer = null
      var bJoin: SimilarityJoin = null
      var rowRDDLarge: RDD[Row] = null

      // weighted Jaccard is ~0.465
      rowRDDLarge = sc.textFile(path + "/dirtyJaccard100dups").map(Row(_))
      blocker = new WeightedJaccardSimilarity(colNames, context, tok, 0.466)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 0)
      blocker = new WeightedJaccardSimilarity(colNames, context, tok, 0.465)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)

      // weighted Overlap is 10
      rowRDDLarge = sc.textFile(path + "/dirtyOverlap100dups").map(Row(_))
      blocker = new WeightedOverlapSimilarity(colNames, context, tok, 10.1)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 0)
      blocker = new WeightedOverlapSimilarity(colNames, context, tok, 10)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)

      // weighted Dice is ~0.776
      rowRDDLarge = sc.textFile(path + "/dirtyDice100dups").map(Row(_))
      blocker = new WeightedDiceSimilarity(colNames, context, tok, 0.777)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 0)
      blocker = new WeightedDiceSimilarity(colNames, context, tok, 0.776)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)

      // weighted Cosine is ~0.473
      rowRDDLarge = sc.textFile(path + "/dirtyCosine100dups").map(Row(_))
      blocker = new WeightedCosineSimilarity(colNames, context, tok, 0.474)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 0)
      blocker = new WeightedCosineSimilarity(colNames, context, tok, 0.473)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)


    }

  }

  test("broadcast sample join un-weighted"){
    withSampleCleanContext { scc =>
      val sc = scc.getSparkContext()
      val context = List("record")
      val colNames = List("record")
      val tok = new DelimiterTokenizer(" ")
      val path = "./src/test/resources"
      var blocker: AnnotatedSimilarityFeaturizer = null
      var bJoin: SimilarityJoin = null
      var rowRDDLarge: RDD[Row] = null
      var rowRDDSmall: RDD[Row] = null


      // 100 duplicates with Jaccard similarity = 0.5
      rowRDDLarge = sc.textFile(path + "/dirtyJaccard100dups").map(Row(_))
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new WeightedJaccardSimilarity(colNames, context, tok, 0.51)
      bJoin = new BroadcastJoin(sc, blocker, false)
      // returns 0 dups
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() == 0)
      blocker = new WeightedJaccardSimilarity(colNames, context, tok, 0.5)
      bJoin = new BroadcastJoin(sc, blocker, false)
      // returns ~50 dups * 2
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() <= 60 * 2)


      // 100 duplicates with Overlap similarity = 5
      rowRDDLarge = sc.textFile(path + "/dirtyOverlap100dups").map(Row(_))
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new WeightedOverlapSimilarity(colNames, context, tok, 6)
      bJoin = new BroadcastJoin(sc, blocker, false)
      // 0 dups
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() == 0)
      blocker = new WeightedOverlapSimilarity(colNames, context, tok, 5)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() <= 60 * 2)

      // 100 duplicates with Dice similarity = 0.8
      rowRDDLarge = sc.textFile(path + "/dirtyDice100dups").map(Row(_))
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new WeightedDiceSimilarity(colNames, context, tok, 0.81)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() == 0)
      blocker = new WeightedDiceSimilarity(colNames, context, tok, 0.8)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2 )
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() <= 60 * 2)


      // 100 duplicates with Cosine similarity = 0.5
      rowRDDLarge = sc.textFile(path + "/dirtyCosine100dups").map(Row(_))
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new WeightedCosineSimilarity(colNames, context, tok, 0.51)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() == 0)
      blocker = new WeightedCosineSimilarity(colNames, context, tok, 0.5)
      bJoin = new BroadcastJoin(sc, blocker, false)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() <= 60 * 2)

    }

  }

  /*test("broadcast sample join weighted"){
    withSampleCleanContext { scc =>
      val sc = scc.getSparkContext()
      val context = List("record")
      val colNames = List("record")
      val tok = new DelimiterTokenizer(" ")
      val path = "./src/test/resources"
      var blocker: AnnotatedSimilarityFeaturizer = null
      var bJoin: SimilarityJoin = null
      var rowRDDLarge: RDD[Row] = null
      var rowRDDSmall: RDD[Row] = null

      // weighted Jaccard is ~0.458
      rowRDDLarge = sc.textFile(path + "/dirtyJaccard100dups").map(Row(_))
      rowRDDSmall = rowRDDLarge.sample(false, 0.5,10).cache()
      blocker = new WeightedJaccardSimilarity(colNames, context, tok, 10)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() == rowRDDSmall.count())
      blocker = new WeightedJaccardSimilarity(colNames, context, tok, 0.457)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2 + rowRDDSmall.count())

      // weighted Overlap is 10
      rowRDDLarge = sc.textFile(path + "/dirtyOverlap100dups").map(Row(_))
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new WeightedOverlapSimilarity(colNames, context, tok, 10.1)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= (rowRDDSmall.count() / 2 - 8))
      blocker = new WeightedOverlapSimilarity(colNames, context, tok, 10)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2 + rowRDDSmall.count() / 2)

      // weighted Dice is ~0.776
      rowRDDLarge = sc.textFile(path + "/dirtyDice100dups").map(Row(_))
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new WeightedDiceSimilarity(colNames, context, tok, 0.777)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() == rowRDDSmall.count())
      blocker = new WeightedDiceSimilarity(colNames, context, tok, 0.776)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2 + rowRDDSmall.count())

      // weighted Cosine is ~0.473
      rowRDDLarge = sc.textFile(path + "/dirtyCosine100dups").map(Row(_))
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new WeightedCosineSimilarity(colNames, context, tok, 0.474)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() == rowRDDSmall.count())
      blocker = new WeightedCosineSimilarity(colNames, context, tok, 0.473)
      bJoin = new BroadcastJoin(sc, blocker, true)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2 + rowRDDSmall.count())

    }
  }*/

  test("Pass Join (self and sample)"){
    withSampleCleanContext { scc =>
      val sc = scc.getSparkContext()
      val context = List("record")
      val colNames = List("record")
      val tok = new DelimiterTokenizer(" ")
      val path = "./src/test/resources"
      var blocker: AnnotatedSimilarityFeaturizer = null
      var bJoin: SimilarityJoin = null
      var rowRDDLarge: RDD[Row] = null
      var rowRDDSmall: RDD[Row] = null

      // self join
      // 100 duplicates with Edit (Levenshtein) similarity = 10
      rowRDDLarge = sc.textFile(path + "/dirtyEdit100dups").map(Row(_))
      blocker = new EditFeaturizer(colNames, context, tok, 9)
      bJoin = new PassJoin(sc, blocker)
      val p = bJoin.join(rowRDDLarge, rowRDDLarge)
      assert(p.count() == 0)
      blocker = new EditFeaturizer(colNames, context, tok, 10)
      bJoin = new PassJoin(sc, blocker)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)

      // sample join
      // 100 duplicates with Edit (Levenshtein) similarity = 10
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new EditFeaturizer(colNames, context, tok, 9)
      bJoin = new PassJoin(sc, blocker)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() == 0)
      blocker = new EditFeaturizer(colNames, context, tok, 10)
      bJoin = new PassJoin(sc, blocker)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2)
      assert(bJoin.join(rowRDDSmall, rowRDDLarge, true).count() <= 60 * 2)

    }
  }

  test("similarity join"){
    withSampleCleanContext{scc =>
      val sc = scc.getSparkContext()
      val context = List("record")
      val colNames = List("record")
      val tok = new DelimiterTokenizer(" ")
      val path = "./src/test/resources"
      var blocker: AnnotatedSimilarityFeaturizer = null
      var simJoin: SimilarityJoin = null
      var rowRDDLarge: RDD[Row] = null
      var rowRDDSmall: RDD[Row] = null

      //self-join
      rowRDDLarge = sc.textFile(path + "/dirtyEdit100dups").map(Row(_))
      blocker = new EditFeaturizer(colNames, context, tok, 9)
      simJoin = new SimilarityJoin(sc,blocker,false)

      val rdd = simJoin.join(rowRDDLarge, rowRDDLarge)
      assert(rdd.count() == 0)
      blocker = new EditFeaturizer(colNames, context, tok, 10)
      simJoin = new SimilarityJoin(sc,blocker,false)
      assert(simJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)

      //sample join
      rowRDDSmall = rowRDDLarge.sample(false, 0.5).cache()
      blocker = new EditFeaturizer(colNames, context, tok, 9)
      simJoin = new SimilarityJoin(sc,blocker,false)
      assert(simJoin.join(rowRDDSmall, rowRDDLarge, true).count() == 0)
      blocker = new EditFeaturizer(colNames, context, tok, 10)
      simJoin = new SimilarityJoin(sc,blocker,false)
      assert(simJoin.join(rowRDDSmall, rowRDDLarge, true).count() >= 40 * 2)
      assert(simJoin.join(rowRDDSmall, rowRDDLarge, true).count() <= 60 * 2)
    }
  }




}
