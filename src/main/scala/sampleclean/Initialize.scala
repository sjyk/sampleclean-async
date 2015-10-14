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
  val SAMPLING_ALC = 1
  
  val RESTAURANT_LOCATION = "./src/main/resources/restaurant.csv"
  val RESTAURANT_SCHEMA = List(("id","String"), ("entity_id","String"), 
                               ("name","String"), ("address","String"), 
                               ("city","String"), ("type","String"))
  val SAMPLING_RESTAURANT = 1


  def queryToJSON(result:Array[Row],dataset:String):JObject= {
      val schema:List[String] = if(dataset == "alcohol") ALC_SCHEMA.map(_._1) else RESTAURANT_SCHEMA.map(_._1)
      val printSchema: List[String] = if(dataset == "alcohol") List("name", "store_location",  "category_name", "vendor",  "item", "description", "pack", "bottle_qty", "total") else List("id","name","address","city","type")
      var json:JObject = ("schema", printSchema) ~ ("query","SELECT * FROM " + dataset)

      var records:List[JObject] = List()
      for(r <- result){
         var count = 2
         var jsonInner:JObject = ("data","records")
         for(s <- schema){
            try{
              jsonInner = jsonInner ~ (s -> r(count).toString())
            }
            catch{
              case ne: NullPointerException => jsonInner = jsonInner ~ (s -> "")
            }
            count = count + 1
         }
         records = jsonInner :: records
      }
      json = json ~ ("records" -> records)
      return json
  }

  def queriesToJSON(result:List[Array[Row]],dataset:String):JArray = {
      var json:JArray = JArray(List(("q0", queryToJSON(result(0),dataset)) ~ ("q1", aggQueryToJSON(result(1),dataset))))
      return json
  }

  def aggQueryToJSON(result:Array[Row], dataset:String):JObject = {
      val schema:List[String] = List("group", "Count")
      val query = if(dataset == "alcohol") "SELECT COUNT(distinct name) FROM alcohol" else "SELECT COUNT(distinct name) FROM restaurant"
      var json:JObject = ("schema", List(schema(1))) ~ ("query",query)

      var sorted = result.sortWith((x,y) => x(1).asInstanceOf[Double] > y(1).asInstanceOf[Double])
      //sorted = if(dataset == "alcohol") sorted.slice(0,10) else sorted

      var records:List[JObject] = List()
      for(r <- sorted){
         var count = 0
         var jsonInner:JObject = ("data","aggregate")
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
    
    //scc.hql("drop index x1 on restaurant_sample_clean")
    //scc.hql("drop index x2 on alcohol_sample_clean")

    scc.closeHiveSession()
    
    scc.hql("drop table restaurant")
    scc.hql("drop table alcohol")

    val alcWS = new CSVLoader(scc, ALC_SCHEMA,ALC_LOCATION).load(SAMPLING_ALC,"alcohol")
    val restaurantWS = new CSVLoader(scc, RESTAURANT_SCHEMA,RESTAURANT_LOCATION).load(SAMPLING_RESTAURANT,"restaurant")

    //scc.hql("CREATE INDEX x1 ON TABLE restaurant_sample_clean(hash) AS 'COMPACT' WITH DEFERRED REBUILD")
    //scc.hql("CREATE INDEX x2 ON TABLE alcohol_sample_clean(hash) AS 'COMPACT' WITH DEFERRED REBUILD")

    //scc.hql("alter index x1 on restaurant_sample_clean rebuild")
    //scc.hql("alter index x2 on alcohol_sample_clean rebuild")

    //scc.hql("SHOW INDEXES ON restaurant_sample_clean").collect().foreach(println)

    val pwaq1 = new PrintWriter(new File("./src/main/resources/alcohol.json"))
    val aq1 = pretty(render(queriesToJSON(List(alcWS.query("select * from $t where hash(id) % 3 = 1 order by name").collect(),
                                               alcWS.query("select 'all',count(distinct name) from $t").collect()),"alcohol")))
    pwaq1.write(aq1)
    pwaq1.close

    val pwrq1 = new PrintWriter(new File("./src/main/resources/restaurant.json"))
    val rq1 = pretty(render(queriesToJSON(List(restaurantWS.query("select * from $t where entity_id % 70 = 1").collect(),
                                                         restaurantWS.query("select 'all',count(distinct name) from $t").collect()),"restaurant")))
    pwrq1.write(rq1)
    pwrq1.close

  }

}
