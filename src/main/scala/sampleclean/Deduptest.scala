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
import sampleclean.clean.deduplication.EntityResolution._

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

import sampleclean.clean.deduplication.EntityResolution._
import sampleclean.eval._
import org.apache.spark.sql.Row

/**
* This object provides the main driver for the SampleClean
* application. We execute commands read from the command 
* line.
*/
object Deduptest {

  def addConstraintsForGroup(e:Evaluator,group: Iterable[Row]) = {
      val groupArray = group.toArray
      
      for(i <- 0 until groupArray.length)
      {   for(j <- 0 until groupArray.length)
          { 
            val row1 = groupArray(i)
            val row2 = groupArray(j)
            if (row1 != row2)
            {
              e.addBinaryConstraint(row1(0).toString(),row2(0).toString(), "name", true)
              //println("test")
            }
          }
      }

  }

  def addPrecisionConstraintsForGroup(e:Evaluator, group: Iterable[Row], allRows: Array[Row]) = {
      val groupSet = group.toArray.toSet
      val groupArray = group.toArray
      
      for(i <- 0 until allRows.length)
      {   val row1 = allRows(i)
          if(!groupSet.contains(row1))
          {
             for(j <- 0 until groupArray.length)
              { 
                val row2 = groupArray(j)
                e.addBinaryConstraint(row1(0).toString(),row2(0).toString(), "name", false)
                //println("test")
              }
            
          }
      }

  }

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

    scc.closeHiveSession()
    val hiveContext = scc.getHiveContext();
    hiveContext.hql("DROP TABLE IF EXISTS restaurant")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS restaurant(id String, entity String,name String,city String,category String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    hiveContext.hql("LOAD DATA LOCAL INPATH 'restaurant.csv' OVERWRITE INTO TABLE restaurant")
    //scc.initializeConsistent("restaurant", "restaurant_data", "id", 1)
    scc.initializeConsistent("restaurant", "restaurant_sample", "entity", 1)

    var algorithm1 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.999, true)
    var algorithm2 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.95, true)
    var algorithm3 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.9, true)
    var algorithm4 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.85, true)
    var algorithm5 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.8, true)
    var algorithm6 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.75, true)
    var algorithm7 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.7, true)
    var algorithm8 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.65, true)
    var algorithm9 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.6, true)
    var algorithm10 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.55, true)
    var algorithm11 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.5, true)
    var algorithm12 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.45, true)
    var algorithm13 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.4, true)
    var algorithm14 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.35, true)
    var algorithm15 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.3, true)
    var algorithm16 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.25, true)
    var algorithm17 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.2, true)
    var algorithm18 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.15, true)
    var algorithm19 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.10, true)
    var algorithm20 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.05, true)
    var algorithm21 = EntityResolution.textAttributeAutomatic(scc, "restaurant_sample", "name", 0.00, true)

    //var algorithm2 = EntityResolution.textAttributeAutomatic(scc, "paper_aff_sample", "affiliation", 0.9, true)
    val e = new Evaluator(scc, List())
    //val e = new Evaluator(scc, List(algorithm1, algorithm1, algorithm2, algorithm3, algorithm4, algorithm5, algorithm6, algorithm7, algorithm8, algorithm9, algorithm10, algorithm11, algorithm12, algorithm13, algorithm14, algorithm15, algorithm16, algorithm17, algorithm18, algorithm19, algorithm20, algorithm21))
    val gt = scc.getCleanSample("restaurant_sample")
    //val allRows = gt.collect()
    //gt.map( x => (x(3).toString(), x)).groupByKey().collect().foreach(x => addPrecisionConstraintsForGroup(e, x._2, allRows))
    gt.map( x => (x(3).toString(), x)).groupByKey().collect().foreach(x => addConstraintsForGroup(e, x._2))
    //val m = new MonotonicSimilarityThresholdTuner(scc,e,algorithm2.components.join.simfeature)
    //val s = new SimilarityMetricChooser(scc,e)
    //println(s.tuneThresholdAndMetric("restaurant_sample",List("name")))
    //println(m.getCandidatePairsCount("restaurant_sample",m.tuneThreshold("restaurant_sample")))

    val cols = List("name")
    val baseFeaturizer = new SimilarityFeaturizer(cols, 
                                                      scc.getTableContext("restaurant_sample"), 
                                                      List("Levenshtein", "JaroWinkler"))
    val s = new CrowdMatcherChooser(scc,e)
    println(s.getCrowdEstimate("restaurant_sample", "name", baseFeaturizer))

    //val e2 = new Evaluator(scc, List(algorithm2))

    //e.addBinaryConstraint("81eb205e-6fe2-48ce-962e-394ffaedbe74","41fb55cb-dd6a-4c43-bc0d-3e619ca07cc2" ,"affiliation", true)
    //e.evaluate("restaurant_sample")
    //e.printResults()

    //algorithm1.components.addMatcher(EntityResolution.createCrowdMatcher(scc,"affiliation","paper_aff_sample"))
    //algorithm1.exec()

    
    /*println("Test 1. Test Automated Entity Resolution")
    var algorithm = EntityResolution.textAttributeAutomatic(scc, "paper_aff_sample", "affiliation", 0.9, false)
    algorithm.exec()

    println("Test 2. Test Active Learning Entity Resolution")
    algorithm = EntityResolution.textAttributeActiveLearning(scc, "paper_aff_sample", "affiliation", 0.9, false)
    algorithm.exec()*/
    
    
  }
  
}
