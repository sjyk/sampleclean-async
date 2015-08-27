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

import sampleclean.eval._

/**
 * This object provides the main driver for the SampleClean
 * application. We execute commands read from the command
 * line.
 */
private [sampleclean] object CleanAndQuery {

  var ALC_SCHEMA = (List("id","date","convenience_store","store","name","address","city","zipcode") :::
                    List("store_location","county_number","county","category","category_name","vendor_no","vendor") :::
                    List("item","description","pack","liter_size","state_btl_cost","btl_price","bottle_qty","total")).map(x => (x, "String"))
  
  var ALC_PSCHEMA = List("name", "store_location",  "category_name", "vendor",  "item", "description", "pack", "bottle_qty", "total")
  var RESTAURANT_PSCHEMA = List("id","name","address","city","type")

  var RESTAURANT_SCHEMA = List(("id","String"), ("entity_id","String"), 
                               ("name","String"), ("address","String"), 
                               ("city","String"), ("type","String"))

    def queryToJSON(result:Array[Row],dataset:String, scc:SampleCleanContext):JObject= {
      val schema:List[String] = if(dataset == "alcohol") ALC_SCHEMA.map(_._1) else RESTAURANT_SCHEMA.map(_._1)
      val printSchema: List[String] = if(dataset == "alcohol")  ALC_PSCHEMA else RESTAURANT_PSCHEMA
      var json:JObject = ("schema", printSchema) ~ ("query","SELECT * FROM " + dataset)
      var records:List[JObject] = List()
      for(r <- result){
         var count = 2
         var jsonInner:JObject = ("data","records")
         for(s <- schema){
              jsonInner = jsonInner ~ (s -> r(count).toString())
            count = count + 1
         }
         records = jsonInner :: records
      }
      json = json ~ ("records" -> records)
      return json
  }

  def aggQueryToJSON(result:Array[Row], dataset:String):JObject = {
      val schema:List[String] = List("group", "Count")
      
      val query = if(dataset == "alcohol") "SELECT COUNT(distinct name) FROM alcohol" else "SELECT COUNT(distinct name) FROM restaurant"
      var json:JObject = ("schema", List(schema(1))) ~ ("query",query)

      var records:List[JObject] = List()
      for(r <- result){
         println(r)
         var count = 0
         //val key = r(0).asInstanceOf[String]
         //val value = r(1).asInstanceOf[Double]

         //if(frMap.contains(key) && frMap(key) != value){
         var jsonInner:JObject = ("data","aggregate")
         for(s <- schema){
               jsonInner = jsonInner ~ (s -> r(count).toString())
            count = count + 1
         }
         records = jsonInner :: records
         //}
      }
      json = json ~ ("records" -> records)
      return json
  }

  def addConstraintsForGroup(e:Evaluator,group: Iterable[Row]) = {
      val groupArray = group.toArray
      
      for(i <- 0 until groupArray.length)
      {   for(j <- 0 until groupArray.length)
          { 
            val row1 = groupArray(i)
            val row2 = groupArray(j)
            if (row1(2).toString() != row2(2).toString())
            {
             // println(row1)
              //println(row2)
              e.addBinaryConstraint(row1(0).toString(),row2(0).toString(), "name", true)
              //println("Added Constraint")
            }
          }
      }

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
      else if (s.options.crowd.get == "hybrid")
      {
         val cm = EntityResolution.createCrowdFilter(scc,s.field,sampleName)
         val er = uninstantiatedAlgo(scc,sampleName)
         er.components.addMatcher(cm)
         uninstantiatedAlgo = ((x:SampleCleanContext,y:String) => er)
      }

      if(s.options.tune.isDefined && s.options.tune.get == "true")
      {
        val e = new Evaluator(scc, List(EntityResolution.longAttributeCanonicalize(scc,sampleName,s.field,s.options.threshold.get)))
        val gtquery = if(sampleName.contains("alcohol")) "select hash, store, name from " else "select hash, entity_id, name from "
        scc.hql(gtquery + sampleName + "_clean").map( x => (x(1).toString(), x)).groupByKey().collect().foreach(x => addConstraintsForGroup(e, x._2))
        val er = uninstantiatedAlgo(scc,sampleName)
        uninstantiatedAlgo = ((x:SampleCleanContext,y:String) => er.tune(e))
      }

      return uninstantiatedAlgo
  }

  def createExtractStage(s:Stage, scc: SampleCleanContext, sampleName:String): (SampleCleanContext,String) => SampleCleanAlgorithm = {
    return FastSplitExtraction.stringSplitAtDelimiter(_,_,s.field,s.options.delimiter.get,s.options.output_columns.get)
  }

  val queries = Map(("attrdedup","alcohol") -> "select 'all',count(distinct name) from $t",
                    ("extract","alcohol") -> "select * from $t where hash(id) % 100 = 1",
                    ("extract","restaurant") -> "select * from $t where hash(name) % 80 = 1",
                    ("attrdedup","restaurant") -> "select 'all',count(distinct name) from $t")

  case class Stages(stages: List[Stage])
  case class Stage(operator: String, field: String, options: OperatorOptions)
  case class OperatorOptions( similarity:Option[String],
                               tune: Option[String],
                               threshold:Option[Double],
                               crowd:Option[String],
                               hybrid_thresh:Option[Double],
                               output_columns:Option[List[String]],
                               delimiter:Option[String])

  case class AggQueryResults(records:List[AggResult])
  case class AggResult(group:String, aggregate:String)

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

    val datasetFile = "./src/main/resources/"+datasetName+".json"
    val results = scala.io.Source.fromFile(datasetFile).mkString
    var resultsJson = JsonMethods.parse(results)

    for (s <- stages.stages){
      if (s.operator == "attrdedup"){
        dataset.clean(createAttrDedupStage(s,scc,datasetName+"_sample"))
      }
      else if (s.operator == "extract"){
        if (datasetName == "alcohol")
        {
            ALC_SCHEMA = ALC_SCHEMA ::: s.options.output_columns.get.map(x => (x, "String"))
            ALC_PSCHEMA = ALC_PSCHEMA ::: s.options.output_columns.get
        }
        else
        {
          RESTAURANT_SCHEMA = RESTAURANT_SCHEMA ::: s.options.output_columns.get.map(x => (x, "String"))
          RESTAURANT_PSCHEMA = RESTAURANT_PSCHEMA ::: s.options.output_columns.get
        }

        dataset.clean(createExtractStage(s,scc,datasetName+"_sample"))
      }

      val result0 = dataset.query(queries(("extract",datasetName))).collect()
      val q0Results = queryToJSON(result0,datasetName,scc)

      //val firstResult = (resultsJson.asInstanceOf[JArray].arr(0) \\ "q1").extract[AggQueryResults]
      //println(pretty(render(jsquad)))
      //val firstResultMap = firstResult.records.map(x => (x.group, 0.0)).toMap

      val result1 = dataset.query(queries(("attrdedup",datasetName))).collect()
      val q1Results = aggQueryToJSON(result1,datasetName)

      val update:JObject = ("q0",q0Results) ~ ("q1",q1Results)

      //val pw = new PrintWriter(new File(datasetFile))
      resultsJson = JArray(resultsJson.asInstanceOf[JArray].arr ::: List(update))
      
      val outputJson = pretty(render(resultsJson))
      val pw = new PrintWriter(new File(datasetFile))
      pw.write(outputJson)
      println(outputJson)
      pw.close

      /*
      //resultsJson = resultsJson merge j1  
      //resultsJson = resultsJson merge j2
      val pw = new PrintWriter(new File(datasetFile))
      pw.write(pretty(render(resultsJson)))
      pw.close
      println(pretty(render(resultsJson)))   
      pcount = pcount + 1*/
      
    }

  }

}
