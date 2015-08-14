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
private [sampleclean] object Initialize {

  val ALC_LOCATION = "./src/main/resources/alcohol.csv"
  val ALC_SCHEMA = (List("id","date","convenience_store","store","name","address","city","zipcode") :::
                    List("store_location","county_number","county","category","category_name","vendor_no","vendor") :::
                    List("item","description","pack","liter_size","state_btl_cost","btl_price","bottle_qty","total")).map(x => (x, "String"))
  val SAMPLING_ALC = 0.0001
  
  val RESTAURANT_LOCATION = "./src/main/resources/restaurant.csv"
  val RESTAURANT_SCHEMA = List(("id","String"), ("entity_id","String"), 
                               ("name","String"), ("address","String"), 
                               ("city","String"), ("type","String"))
  val SAMPLING_RESTAURANT = 1


  def queryToJSON(result:Array[Row],dataset:String):JObject= {
      val schema:List[String] = if(dataset == "alcohol") ALC_SCHEMA.map(_._1) else RESTAURANT_SCHEMA.map(_._1)
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
    
    scc.closeHiveSession()
    scc.hql("drop table restaurant")
    scc.hql("drop table alcohol")

    val alcWS = new CSVLoader(scc, ALC_SCHEMA,ALC_LOCATION).load(SAMPLING_ALC,"alcohol")
    val restaurantWS = new CSVLoader(scc, RESTAURANT_SCHEMA,RESTAURANT_LOCATION).load(SAMPLING_RESTAURANT,"restaurant")

    val pwaq1 = new PrintWriter(new File("./src/main/resources/alc1.json"))
    val aq1 = pretty(render(queryToJSON(alcWS.query("select * from $t limit 10").collect(),"alcohol")))
    pwaq1.write(aq1)
    pwaq1.close

    val pwaq2 = new PrintWriter(new File("./src/main/resources/alc2.json"))
    val aq2 = pretty(render(aggQueryToJSON(alcWS.query("selectrawsc count(1) from $t group by name").collect())))
    pwaq2.write(aq2)
    pwaq2.close

    val pwrq1 = new PrintWriter(new File("./src/main/resources/restaurant1.json"))
    val rq1 = pretty(render(queryToJSON(restaurantWS.query("select * from $t limit 10").collect(),"restaurant")))
    pwrq1.write(rq1)
    pwrq1.close

    val pwrq2 = new PrintWriter(new File("./src/main/resources/restaurant2.json"))
    val rq2 = pretty(render(aggQueryToJSON(restaurantWS.query("select 'all',count(distinct name) from $t").collect())))
    pwrq2.write(rq2)
    pwrq2.close

     val pwr1 = new PrintWriter(new File("./src/main/resources/restaurant2res.json"))
     pwr1.close

     val pwr2 = new PrintWriter(new File("./src/main/resources/alc1res.json"))
     pwr2.close

     val pwr3 = new PrintWriter(new File("./src/main/resources/alc2res.json"))
     pwr3.close

  }

}
