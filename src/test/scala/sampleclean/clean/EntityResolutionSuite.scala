package sampleclean.clean

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.FunSuite
import sampleclean.api.SampleCleanContext
import sampleclean.clean.algorithm.AlgorithmParameters
import sampleclean.clean.deduplication.join.{BlockerMatcherSelfJoinSequence, BroadcastJoin}
import sampleclean.clean.deduplication.matcher.AllMatcher
import sampleclean.clean.deduplication.{EntityResolution, GraphXInterface}
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer.WeightedJaccardSimilarity
import sampleclean.clean.featurize.Tokenizer.DelimiterTokenizer


class EntityResolutionSuite extends FunSuite with LocalSCContext {
  val attr = "col0"
  val colNames = List(attr)
  val tok = new DelimiterTokenizer(" ")
  val sampleTableName = "test_sample"


  def defaultBM(scc:SampleCleanContext,sim: AnnotatedSimilarityFeaturizer): BlockerMatcherSelfJoinSequence = {
      val bJoin = new BroadcastJoin(scc.getSparkContext(), sim, false)
      val matcher = new AllMatcher(scc, sampleTableName)
      new BlockerMatcherSelfJoinSequence(scc, sampleTableName, bJoin, List(matcher))

  }


  test("exec") {
    withSingleAttribute (1,{scc =>
      // Initialize algorithm
      val similarity = new WeightedJaccardSimilarity(colNames, scc.getTableContext(sampleTableName), tok, 0.5)
      val params = new AlgorithmParameters()
      params.put("attr", attr)
      params.put("mergeStrategy", "mostFrequent")

      val ER = new EntityResolution(params, scc, sampleTableName, defaultBM(scc, similarity))

      // Within exec() function
      ER.setTableParameters(sampleTableName)
      assert(ER.attrCol == 3 && ER.hashCol == 0)
      val sampleTableRDD = scc.getCleanSample(sampleTableName)
      val attrCountGroup = sampleTableRDD.map(x =>
        (x(ER.attrCol).asInstanceOf[String],
          x(ER.hashCol).asInstanceOf[String])).
        groupByKey()
      val attrCountRdd = attrCountGroup.map(x => Row(x._1, x._2.size.toLong))
      val vertexRDD = attrCountGroup.map(x => (x._1.hashCode().toLong,
        (x._1, x._2.toSet)))

      ER.components.updateContext(List(attr, "count"))

      val edgeRDD: RDD[(Long, Long, Double)] = scc.getSparkContext().parallelize(List())
      ER.graphXGraph = GraphXInterface.buildGraph(vertexRDD, edgeRDD)

      assert(sampleTableRDD.count() == 201)
      assert(attrCountGroup.count() == 201)
      assert(attrCountRdd.count() == 201)
      assert(vertexRDD.count() == 201)
      assert(edgeRDD.count() == 0)

      ER.components.printPipeline()
      ER.components.setOnReceiveNewMatches(ER.apply)

      val candidates = ER.components.blockAndMatch(attrCountRdd)
      assert(candidates.count() == 100)
      ER.apply(candidates)

      assert(scc.getCleanSampleAttr(sampleTableName, "col0").map(x => (x.getString(1), x.getString(0))).groupByKey().count() == 101)
    })


  }

  test("api") {
    withSingleAttribute (1,{ scc =>
      // Initialize algorithm
      //scc.resetSample(sampleTableName)
      val params = new AlgorithmParameters()
      params.put("attr", attr)
      params.put("mergeStrategy", "mostFrequent")

      val similarity = new WeightedJaccardSimilarity(colNames, scc.getTableContext(sampleTableName), tok, 0.5)

      val ER = new EntityResolution(params, scc, sampleTableName, defaultBM(scc, similarity))

      assert(scc.getCleanSampleAttr(sampleTableName, "col0").map(x => (x.getString(1), x.getString(0))).groupByKey().count() == 201)
      ER.exec()
      assert(scc.getCleanSampleAttr(sampleTableName, "col0").map(x => (x.getString(1), x.getString(0))).groupByKey().count() == 101)
    })

  }

  test("overhead") {
    withSingleAttribute (1,{ scc =>
      //scc.resetSample(sampleTableName)
      val params = new AlgorithmParameters()

      var t0 = System.nanoTime()
      params.put("attr", attr)
      params.put("mergeStrategy", "mostFrequent")
      val similarity = new WeightedJaccardSimilarity(colNames, scc.getTableContext(sampleTableName).drop(2), tok, 0.5)
      val ER = new EntityResolution(params, scc, sampleTableName, defaultBM(scc, similarity))
      val t01 = System.nanoTime()
      ER.exec()
      var t1 = System.nanoTime()

      println("Exec() in algorithm lasted " + (t1 - t01).toDouble / 1000000000 + " seconds.")
      println("Whole cleaning algorithm lasted " + (t1 - t0).toDouble / 1000000000 + " seconds.")

      val rowRDDLarge = scc.getSparkContext().textFile("./src/test/resources/csvJaccard100dupsAttr").map(x => Row.fromSeq(x.split(",", -1).toSeq))

      t0 = System.nanoTime()
      val blocker = new WeightedJaccardSimilarity(colNames, scc.getTableContext(sampleTableName).drop(2), tok, 0.5)
      val bJoin = new BroadcastJoin(scc.getSparkContext(), blocker, false)
      assert(bJoin.join(rowRDDLarge, rowRDDLarge).count() == 100)
      t1 = System.nanoTime()

      println("Join lasted " + (t1 - t0).toDouble / 1000000000 + " seconds.")
    })


  }


}
