package sampleclean

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanAQP;
import org.apache.spark.sql.hive.HiveContext
import sampleclean.util._

import sampleclean.clean.deduplication.join._
import sampleclean.clean.deduplication.blocker._
import sampleclean.clean.deduplication.matcher._
import sampleclean.clean.deduplication._
import sampleclean.clean.deduplication.EntityResolution._
import sampleclean.clean.extraction.SplitExtraction._
import sampleclean.clean.extraction.SplitExtraction

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
private [sampleclean] object VLDBDemo {

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

    scc.closeHiveSession()
    val hiveContext = scc.getHiveContext();
    val loader = new CSVLoader(scc, List(("id","String"),("entity","String"),("name","String"),("address","String"),("city","String"),("category","String")),"restaurant.csv")
    val data = loader.load()

    //scc.hql("show tables").collect().foreach(println)

    //setup evaluation
    val e = new Evaluator(scc)
    data.query("select * from $t").map( x => (x(3).toString(), x)).groupByKey().collect().foreach(x => addConstraintsForGroup(e, x._2))


    data.clean(EntityResolution.longAttributeCanonicalize(_,_,"name", 0.6).tune(e))
    //data.clean(EntityResolution.longAttributeCanonicalize(_,_,"name", 0.6).auto(e))

        .clean(SplitExtraction.stringSplitAtDelimiter(_,_,
                                  "address", 
                                  "between", 
                                  List("streetaddress", "crossroad")))

        .query("SELECT COUNT(1) FROM $t")

        .collect()

  }
  
}
