package sampleclean

import org.apache.spark.{SparkContext, SparkConf}

import sampleclean.clean.algorithm.{SampleCleanAlgorithm}
import sampleclean.clean.extraction.SplitExtraction
import sampleclean.eval._

import sampleclean.util._
import sampleclean.api.{WorkingSet, SampleCleanContext}
import sampleclean.clean.deduplication._
import org.apache.spark.sql.Row

import org.json4s._
import org.json4s.native.JsonMethods
import org.json4s.native.JsonMethods._

/**
 * This object provides the main driver for the SampleClean
 * application. We execute commands read from the command
 * line.
 */
private [sampleclean] object ALdemo {

  case class UnparsedAlgorithm(name:String,
                               threshold:Option[Double],
                               attribute:Option[String],
                               weighting:Option[Boolean],
                               second_threshold:Option[Double],
                               dedup_columns:Option[List[String]],
                               output_columns:Option[List[String]],
                               delimiter:Option[String]){

    def toSCAlgorithm: (SampleCleanContext,String) => SampleCleanAlgorithm = {
      name match {
        case "shortAttributeER" => EntityResolution.shortAttributeCanonicalize(_,_,attribute.get,threshold.get)
        case "longAttributeER" => EntityResolution.longAttributeCanonicalize(_,_,attribute.get,threshold.get,weighting.get)
        case "activeLearningER" => EntityResolution.textAttributeActiveLearning(_,_,attribute.get,threshold.get,weighting.get)
        case "hybridER" => EntityResolution.hybridAttributeAL(_,_,attribute.get,threshold.get,second_threshold.get,weighting.get)
        case "recordDedup" => RecordDeduplication.deduplication(_,_,dedup_columns.get,threshold.get,weighting.get)
        case "splitExtraction" => SplitExtraction.stringSplitAtDelimiter(_,_,attribute.get,delimiter.get,output_columns.get)
        case _ => throw new RuntimeException("Algorithm name not found.")
      }

    }

  }

  case class UnparsedPipeline(pipeline: List[UnparsedAlgorithm])

  /**
   * Main function
   */
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("SampleClean Spark Driver")
    conf.setMaster("local[4]")
    conf.set("spark.executor.memory", "4g")

    val sc = new SparkContext(conf)
    val scc = new SampleCleanContext(sc)
    scc.closeHiveSession()
    println("closed hive session")
    val hiveContext = scc.getHiveContext()

    val source = scala.io.Source.fromFile("./src/main/resources/vldb_input_test.json").mkString

    val json = JsonMethods.parse(source)
    implicit val formats = DefaultFormats

    val dataset:WorkingSet = ((json \\ "dataset").values.toString match {
      case "restaurant" => new CSVLoader(scc, List(("id","String"), ("entity_id","String"), ("name","String"), ("address","String"), ("city","String"), ("type","String")),
        "./src/main/resources/restaurant.csv" )
      //TODO
      case "alcohol" => new CSVLoader(scc, List(("id","String"), ("entity_id","String"), ("name","String"), ("address","String"), ("city","String"), ("type","String")),
        "alcohol.csv" )
    }).load()

    val algorithms = json.extract[UnparsedPipeline]

    for (a <- algorithms.pipeline){
      dataset.clean(a.toSCAlgorithm)
      println("Finished " + a.name + " algorithm")
    }

  }

}
