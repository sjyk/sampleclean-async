package sampleclean

import org.apache.spark.{SparkContext, SparkConf}

import sampleclean.clean.algorithm.{SampleCleanAlgorithm}
import sampleclean.clean.extraction.FastSplitExtraction
import sampleclean.eval._

import sampleclean.util._
import sampleclean.api.{WorkingSet, SampleCleanContext}
import sampleclean.clean.deduplication._
import org.apache.spark.sql.Row

import org.json4s._
import org.json4s.native.JsonMethods
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._

import java.io._

/**
 * This object provides the main driver for the SampleClean
 * application. We execute commands read from the command
 * line.
 */
private [sampleclean] object CleanAndQuery {

  val ALC_SCHEMA = (List("id","date","convenience_store","store","name","address","city","zipcode") :::
                    List("store_location","county_number","county","category","category_name","vendor_no","vendor") :::
                    List("item","description","pack","liter_size","state_btl_cost","btl_price","bottle_qty","total")).map(x => (x, "String"))
  
  val RESTAURANT_SCHEMA = List(("id","String"), ("entity_id","String"), 
                               ("name","String"), ("address","String"), 
                               ("city","String"), ("type","String"))

    def queryToJSON(result:Array[Row],dataset:String, scc:SampleCleanContext):JObject= {
      val schema:List[String] = scc.getTableContext(dataset+"_sample")
      var json:JObject = ("schema", schema)
      var records:List[JObject] = List()
      for(r <- result){
         var count = 0
         var jsonInner:JObject = ("data","record")
         for(s <- schema){
            jsonInner = jsonInner ~ (s -> r(count).toString())
            count = count + 1
         }
         records = jsonInner :: records
      }
      json = json ~ ("records" -> records)
      return json
  }

  def aggQueryToJSON(result:Array[Row]):JObject = {
      val schema:List[String] = List("group", "aggregate")
      var json:JObject = ("schema", schema)
      var records:List[JObject] = List()
      for(r <- result){
         println(r)
         var count = 0
         var jsonInner:JObject = ("data","record")
         for(s <- schema){
            jsonInner = jsonInner ~ (s -> r(count).toString())
            count = count + 1
         }
         records = jsonInner :: records
      }
      json = json ~ ("records" -> records)
      return json
  }

  def createAttrDedupStage(s:Stage, scc: SampleCleanContext, sampleName:String): (SampleCleanContext,String) => SampleCleanAlgorithm = {
      var uninstantiatedAlgo: (SampleCleanContext,String) => EntityResolution = null 

      if(s.options.similarity.get == "edit")
        uninstantiatedAlgo = EntityResolution.shortAttributeCanonicalize(_,_,s.field,s.options.threshold.get)
      else if (s.options.similarity.get == "jaccard")
        uninstantiatedAlgo = EntityResolution.longAttributeCanonicalize(_,_,s.field,s.options.threshold.get)
         //else if (s.options.similarity.get == "overlap")
         //   uninstantiatedAlgo = EntityResolution.longAttributeCanonicalize(_,_,s.field,s.options.threshold.get).changeSimilarity("WeightedOverlap")
      else if (s.options.similarity.get == "cosine")
        uninstantiatedAlgo = EntityResolution.longAttributeCanonicalize(_,_,s.field,s.options.threshold.get).changeSimilarity("WeightedCosine")
    
      if (s.options.crowd.get == "active")
      {
         val cm = EntityResolution.createCrowdMatcher(scc,s.field,sampleName)
         val er = uninstantiatedAlgo(scc,sampleName)
         er.components.addMatcher(cm)
         uninstantiatedAlgo = ((x:SampleCleanContext,y:String) => er)
      }
      /*else if (s.options.crowd.get == "hybrid")
      {
         uninstantiatedAlgo = EntityResolution.hybridAttributeAL(_,_,attribute.get,threshold.get,second_threshold.get,weighting.get)
      }*/

      return uninstantiatedAlgo
  }

  def createExtractStage(s:Stage, scc: SampleCleanContext, sampleName:String): (SampleCleanContext,String) => SampleCleanAlgorithm = {
    return FastSplitExtraction.stringSplitAtDelimiter(_,_,s.field,s.options.delimiter.get,s.options.output_columns.get)
  }

  val queries = Map(("attrdedup","alcohol") -> "selectrawsc count(1) from $t group by name",
                    ("extract","alcohol") -> "select * from $t limit 10",
                    ("attrdedup","restaurant") -> "select 'all',count(distinct name) from $t")

  val outputFiles = Map(("attrdedup","alcohol") -> "alc2res.json",
                    ("extract","alcohol") -> "alc1res.json",
                    ("attrdedup","restaurant") -> "restaurant2res.json")

  case class Stages(stages: List[Stage])
  case class Stage(operator: String, field: String, options: OperatorOptions)
  case class OperatorOptions( similarity:Option[String],
                               threshold:Option[Double],
                               crowd:Option[String],
                               hybrid_thresh:Option[Double],
                               output_columns:Option[List[String]],
                               delimiter:Option[String])

  /**
   * Main function
   */
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("SampleClean Spark Driver")
    conf.setMaster("local[4]")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "1g")
    conf.set("spark.storage.memoryFraction", "0.2")

    val sc = new SparkContext(conf)
    val scc = new SampleCleanContext(sc)
    val hiveContext = scc.getHiveContext()
  
    val source = scala.io.Source.fromFile("./src/main/resources/vldb_input_test.json").mkString
    //val source = scala.io.Source.fromFile(args(0)).mkString

    val json = JsonMethods.parse(source)
    implicit val formats = DefaultFormats
    val datasetName = (json \\ "dataset").values.toString

    val dataset:WorkingSet = datasetName match {
      case "restaurant" => new WorkingSet(scc, "restaurant_sample")
      case "alcohol" => new WorkingSet(scc, "alcohol_sample")
    }

    val stages = json.extract[Stages]

    for (s <- stages.stages){
      if (s.operator == "attrdedup"){
        dataset.clean(createAttrDedupStage(s,scc,datasetName+"_sample"))
        val result = dataset.query(queries((s.operator,datasetName))).collect()
        val pw = new PrintWriter(new File("./src/main/resources/"+outputFiles((s.operator,datasetName))))
        pw.write(pretty(render(aggQueryToJSON(result))))
        pw.close
        
      }
      else if (s.operator == "extract"){
        dataset.clean(createExtractStage(s,scc,datasetName+"_sample"))
        val result = dataset.query(queries((s.operator,datasetName))).collect()
        val pw = new PrintWriter(new File("./src/main/resources/"+outputFiles((s.operator,datasetName))))
        pw.write(pretty(render(queryToJSON(result,datasetName,scc))))
        pw.close
      }
      
    }

  }

}
