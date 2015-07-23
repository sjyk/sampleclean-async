package sampleclean

import org.apache.spark.{SparkContext, SparkConf}
import sampleclean.eval._

import sampleclean.util._
import sampleclean.api.SampleCleanContext
import sampleclean.clean.deduplication._
import org.apache.spark.sql.Row

/**
 * This object provides the main driver for the SampleClean
 * application. We execute commands read from the command
 * line.
 */
private [sampleclean] object ALdemo {


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
    println("closed hive session")
    val hiveContext = scc.getHiveContext();
    //val loader = new CSVLoader(scc, List(("id","String"),("name","String")),"/Users/juanmanuelsanchez/Documents/sampleCleanData/hybridMatcherTest")
    val loader = new CSVLoader(scc, List(("id","String"), ("entity_id","String"), ("name","String"), ("address","String"), ("city","String"), ("type","String")),
    "/Users/juanmanuelsanchez/src/sampleclean-async/src/main/resources/restaurant.csv" )
    val data = loader.load()


    val attribute = "name"

    def hybridER(scc:SampleCleanContext,sampleName:String) =
      EntityResolution.hybridAttributeAL(scc,sampleName,attribute,0.5,0.9,false)

    def alER(scc:SampleCleanContext,sampleName:String) =
      EntityResolution.textAttributeActiveLearning(scc,sampleName,attribute,0.9,false)

    data.clean(hybridER)


  }

}
