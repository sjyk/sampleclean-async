package sampleclean

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanAQP;
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
//import sampleclean.clean.extraction.LearningSplitExtraction

import sampleclean.clean.deduplication.EntityResolution._
import sampleclean.eval._
import org.apache.spark.sql.Row

/**
* This object provides the main driver for the SampleClean
* application. We execute commands read from the command 
* line.
*/
private [sampleclean] object Deduptest {

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
    scc.hql("DROP TABLE IF EXISTS restaurant")
    scc.hql("CREATE TABLE IF NOT EXISTS restaurant(id String, entity String,name String,category String,city String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    scc.hql("LOAD DATA LOCAL INPATH 'restaurant.csv' OVERWRITE INTO TABLE restaurant")
    //scc.initializeConsistent("restaurant", "restaurant_data", "id", 1)
    
    scc.initialize("restaurant","restaurant_sample")

    scc.hql("select count(distinct name) from restaurant").collect().foreach(println)
    
    var algorithm1 = EntityResolution.longAttributeCanonicalize(scc, "restaurant_sample", "name", 0.7)
    //algorithm1.components.changeSimilarity("EditDistance")
    algorithm1.exec()
    
    scc.writeToParent("restaurant_sample")
    
     //scc.hql("select count(name) from restaurant").collect().foreach(println)
     scc.hql("select count(distinct name) from restaurant").collect().foreach(println)

    //var algorithm1 = EntityResolution.longAttributeCanonicalize(scc, "restaurant_sample", "name", 0.9)
    //algorithm1.components.changeSimilarity("EditDistance")
    //algorithm1.exec()



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
