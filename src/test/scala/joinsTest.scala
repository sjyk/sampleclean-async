/**
 * Created by juanmanuelsanchez on 12/8/14.
 */

package dedupTesting

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row

import org.scalatest.FunSuite
import org.apache.spark._

import org.apache.spark.SparkContext._
import org.apache.spark.util.Utils

import sampleclean.clean.deduplication._
import sampleclean.clean.featurize.Tokenizer.WordTokenizer
import sampleclean.clean.featurize.{Tokenizer, SimilarityFeaturizer, WeightedJaccardBlocking}
import sampleclean.simjoin.BroadcastJoin


class JoinsTest extends FunSuite with Serializable {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("SCUnitTest")
  val sc = new SparkContext(conf)

  val rowRDDLarge = sc.parallelize(Seq("a B c c", "a a C f", "ty y", "a b c c", "")).map(x => Row(x))


  test("edit distance"){

    val key = BlockingKey(Seq(0),WordTokenizer(), true)
    val editJoin = new PassJoin

    assert(editJoin.isSimilar("abcd","abc",1) === true)
    assert(editJoin.genEvenSeg(4,2) === Vector((0,0,1,4), (1,1,1,4), (2,2,2,4)))
    assert(editJoin.genOptimalSubs(4,2,false) === Vector((0,0,0,2), (1,1,1,2), (2,3,1,2), (0,0,1,3), (1,1,1,3), (1,2,1,3), (2,3,1,3), (0,0,1,4), (1,0,1,4), (1,1,1,4), (1,2,1,4), (2,2,2,4), (0,0,1,5), (1,0,2,5), (1,1,2,5), (2,2,2,5), (0,0,2,6), (1,1,2,6), (2,2,2,6)))
    assert(editJoin.genOptimalSubs(4,2,true) === Vector((0,0,0,2), (1,1,1,2), (2,3,1,2), (0,0,1,3), (1,1,1,3), (1,2,1,3), (2,3,1,3), (0,0,1,4), (1,0,1,4), (1,1,1,4), (1,2,1,4), (2,2,2,4)))
    assert(editJoin.genSubstrings(4,4,2) === Vector((0,0,1,4), (1,0,1,4), (1,1,1,4), (1,2,1,4), (2,2,2,4)))

    val rowRDDselfJoin = editJoin.broadcastJoin(sc,2.0, rowRDDLarge, key)
    val pairs = rowRDDselfJoin.collect().toSeq
    assert(rowRDDselfJoin.count() === 3)
    val answer1 = Array(("a b c c","a a C f"), ("a b c c", "a B c c"), ("a a C f", "a B c c")).map(x => (Row(x._1), Row(x._2)))
    assert(pairs === answer1)

    val rowRDDsmall = sc.parallelize(Seq("a b c d", " y","")).map(x => Row(x))
    val rowRDDsampleJoin = editJoin.broadcastJoin(sc,2.0, rowRDDLarge, key, rowRDDsmall, key)

    val pairs2 = rowRDDsampleJoin.collect().toSeq
    assert(rowRDDsampleJoin.count() === 5)
    val answer2 = Array(("",""), ("a B c c", "a b c d"), ("a a C f","a b c d"), ("", " y"), ("a b c c","a b c d")).map(x => (Row(x._1), Row(x._2)))
    assert(pairs2 === answer2)

  }

  test("broadcast join") {

    val jaccBlock = new WeightedJaccardBlocking(List(0),WordTokenizer(),0.5)

    val RDDLarge = sc.parallelize(Seq("a b c c", "a b c c", "a a C f")).map(x => Row(x))
    val RDDSmall = sc.parallelize(Seq("a b c c","a a C f")).map(x => Row(x))

    val simpleLarge = sc.parallelize(Seq("a","b","c","a")).map(x => Row(x))
    val simpleSmall = sc.parallelize(Seq("a","b","a")).map(x => Row(x))

    val bJoin = new BroadcastJoin(sc,jaccBlock,List(0))

    val sampleJ = bJoin.join(simpleSmall,simpleLarge).collect().toSeq
    val selfJJ = bJoin.join(simpleLarge,simpleLarge).collect().toSeq

    //assert(sampleJ == Seq((Row("a b c c"),Row("a b c c")), (Row("a b c c"),Row("a b c c")), (Row("a a C f"),Row("a a C f"))))

    val selfJ = bJoin.join(RDDLarge,RDDLarge).collect().toSeq
    assert(selfJ == Seq((Row("a b c c"),Row("a b c c"))))

    val unionJ = bJoin.join(RDDSmall,RDDLarge,containment = false).collect().toSeq
    assert(unionJ == Seq((Row("a b c c"),Row("a b c c")), (Row("a b c c"),Row("a b c c")), (Row("a a C f"),Row("a a C f"))))

    // Weighted joins
    val wJoin = new BroadcastJoin(sc,jaccBlock,List(0),true)

    val sampleJW = wJoin.join(RDDSmall,RDDLarge).collect().toSeq
    assert(sampleJW == Seq((Row("a b c c"),Row("a b c c")), (Row("a b c c"),Row("a b c c")), (Row("a a C f"),Row("a a C f"))))

    val selfJW = wJoin.join(RDDLarge,RDDLarge).collect().toSeq
    assert(selfJW == Seq((Row("a b c c"),Row("a b c c"))))

    val unionJW = wJoin.join(RDDSmall,RDDLarge,containment = false).collect().toSeq
    assert(unionJW == Seq((Row("a b c c"),Row("a b c c")), (Row("a b c c"),Row("a b c c")), (Row("a a C f"),Row("a a C f"))))

  }





}
