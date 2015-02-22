package sampleclean

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanAQP;
import sampleclean.parse.SampleCleanParser;
import org.apache.spark.sql.hive.HiveContext

import sampleclean.clean.deduplication.join._
import sampleclean.clean.deduplication.blocker._
import sampleclean.clean.deduplication.matcher._
import sampleclean.clean.deduplication._

import sampleclean.activeml._
import sampleclean.api.{SampleCleanAQP, SampleCleanContext, SampleCleanQuery}
import sampleclean.clean.algorithm.{AlgorithmParameters, SampleCleanAlgorithm, SampleCleanPipeline}
import sampleclean.clean.deduplication.{ActiveLearningStrategy, CrowdsourcingStrategy, _}
import sampleclean.crowd.{CrowdConfiguration, CrowdTaskConfiguration}
import sampleclean.clean.featurize.SimilarityFeaturizer
import sampleclean.clean.featurize.AnnotatedSimilarityFeaturizer._
import sampleclean.clean.featurize.LearningSimilarityFeaturizer
import sampleclean.clean.featurize.Tokenizer._
import sampleclean.clean.extraction.LearningSplitExtraction

/**
* This object provides the main driver for the SampleClean
* application. We execute commands read from the command 
* line.
*/
object Deduptest {

  /**
   * Main function
   */
  def main(args: Array[String]) {

    val conf = new SparkConf();
    conf.setAppName("SampleClean Spark Driver");
    conf.setMaster("local[4]");
    conf.set("spark.executor.memory", "4g");

    val sc = new SparkContext(conf);
    val scc = new SampleCleanContext(sc);
    val saqp = new SampleCleanAQP();

    println("Test 1. Test Automated Entity Resolution")
    val algoPara = new AlgorithmParameters()
    algoPara.put("attr", "affiliation")
    algoPara.put("mergeStrategy", "mostFrequent")

    val similarity = new WeightedJaccardSimilarity(List("affiliation"), 
                                                   scc.getTableContext("paper_aff_sample"),
                                                   WordTokenizer(), 
                                                   0.9)

    val join = new BroadcastJoin(sc, similarity, true)
    val cols = List("affiliation")
    val colNames = List("affiliation")
    val baseFeaturizer = new SimilarityFeaturizer(cols, scc.getTableContext("paper_aff_sample"), List("Levenshtein", "JaroWinkler"))
    val alStrategy = new ActiveLearningStrategy(colNames, baseFeaturizer)
    val matcher = new ActiveLeaningMatcher(scc, "paper_aff_sample", alStrategy)
    val blockerMatcher = new BlockerMatcherSelfJoinSequence(scc,"paper_aff_sample", join, List(matcher))
    val algorithm = new EntityResolution(algoPara, scc, "paper_aff_sample", blockerMatcher)
    algorithm.exec()
    

  }
  
}
