package sampleclean.parse

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanAQP;
import sampleclean.api.SampleCleanQuery;
import sampleclean.api.SampleCleanContext._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import sampleclean.clean.outlier.ParametricOutlier;
import sampleclean.clean.misc._
import sampleclean.clean.deduplication._
import sampleclean.activeml._
import sampleclean.clean.algorithm.AlgorithmParameters
import sampleclean.clean.algorithm.SampleCleanPipeline

import sampleclean.clean.deduplication.WordTokenizer
import sampleclean.clean.deduplication.BlockingStrategy
import sampleclean.clean.deduplication.BlockingKey
import sampleclean.clean.deduplication.ActiveLearningStrategy

/* This class parses a command 
*  it takes an SC command argument
*  and tokenizes it.
*/
@serializable
class SampleCleanParser(scc: SampleCleanContext, saqp:SampleCleanAQP) {

  /* This function tokenizes reserved commands
   */
  def reservedCommandTokenizer(command:String):List[String] = {

  		val splitComponents = command.split("\\s+")
  		var result = List[String]()
  		
  		var mergeBuffer = ""

  		for(i <- splitComponents)
  		{
  			val stringSignal = i.indexOf("'")

  			if(mergeBuffer.equals("") &&
  				stringSignal >= 0)
  			{
  				if(i.indexOf("'",stringSignal+1) >= 0)
  					result = i.replaceAll("'","") :: result
  				else
  					mergeBuffer = mergeBuffer + " " + i
  			}
  			else if(!mergeBuffer.equals("") &&
  				stringSignal >= 0)
  			{
  				mergeBuffer = mergeBuffer + " " + i
  				result = mergeBuffer.replaceAll("'","") :: result
  				mergeBuffer = ""
  			}
  			else {
  				result = i :: result
  			}
  			
  		}
  		return result.reverse
  }

  /* SampleClean Query Parser
   */
   def queryParser(command:String):SampleCleanQuery={

   		val from = command.indexOf("from")

   		if(from < 0)
   			return null

   		val where = command.indexOf("where")
   		val groupby = command.indexOf("group by")
   		val expr = command.substring(0,from)
   		
   		var table, pred, group = ""

   		if(where < 0 && groupby < 0){
   			table = command.substring(from+5)
   			pred = ""
   			group = ""
   		}
   		else if(groupby < 0){
   			table = command.substring(from+4,where)
   			pred = command.substring(where+5)
   			group = ""
   		}
   		else if(where < 0){
   			table = command.substring(from+4,groupby)
   			group = command.substring(groupby+8)
   			pred = ""
   		}
   		else{
   			table = command.substring(from+4,where)
   			pred = command.substring(where+5, groupby)
   			group = command.substring(groupby+8)
   		}
   		val parsedExpr = expressionParser(expr)

   		return new SampleCleanQuery(scc,
   									saqp,
   									table,
   									parsedExpr._2,
   									parsedExpr._1,
   									pred,
   									group)
   		
   }

   /* SampleClean Expression Parser
   */
   def expressionParser(command:String):(String, String) ={
   		val splitComponents = command.split("\\s+")
   		val exprClean = splitComponents.drop(1).mkString(" ")
   		val attrStart = exprClean.indexOf("(")
   		val attrEnd = exprClean.indexOf(")")

   		if(attrStart == -1 || attrEnd == -1)
   			return ("", "")
   		else{
   			var attr = exprClean.substring(attrStart+1,attrEnd).trim()

   			if(attr == "1") //always set to a col name
   				attr = "hash"

   			return (exprClean.substring(0,attrStart),attr)
   		}
   }

  /* This command returns a result and a code tuple.
   */
  def parseAndExecute(commandI:String):(String, Long) = {

  	val now = System.nanoTime
  	val command = commandI.replace(";","").toLowerCase().trim()

	if(command.equals("load demo"))
  	{
  		initDemo()
  		return ("Demo Initialized", (System.nanoTime - now)/1000000)
  	}
  	else if(command.substring(0,4).equals("init"))
  	{
   		val comps = reservedCommandTokenizer(command)
  		val name = comps(1)
  		val bt = comps(2)
  		val samplingRatio = comps(3).toDouble
  		scc.initialize(bt,name,samplingRatio,true)
  		return("Create SC Table: " + name, (System.nanoTime - now)/1000000)
  	}
  	else if(command.substring(0,5).equals("merge"))
  	{
  		val comps = reservedCommandTokenizer(command)
  		val name = comps(1)
  		val attr = comps(2)
  		val key1 = comps(3)
  		val key2 = comps(4)

  		val algoPara = new AlgorithmParameters()
    	algoPara.put("attr", attr)

   		algoPara.put("src", key1)
    	algoPara.put("target", key2)
    	val d = new MergeKey(algoPara,scc);
    	d.blocking = false
    	d.name = "Merge"
    	val pp = new SampleCleanPipeline(saqp,List(d))
    	pp.exec(name)

  		return ("Merged Keys", (System.nanoTime - now)/1000000)
  	}
  	else if(command.substring(0,5).equals("dedup"))
  	{
  		val sampleKey = new BlockingKey(Seq(4,5,6,7),WordTokenizer())
    	val fullKey = new BlockingKey(Seq(2,3,4,5),WordTokenizer())

    	def toPointLabelingContext(sampleRow1: Row, fullRow2: Row): PointLabelingContext = {
     	 val sampleData = List(sampleRow1.getString(2),
                            sampleRow1.getString(3),
                            sampleRow1.getString(4),
                            sampleRow1.getString(5),
                            sampleRow1.getString(6),
                            sampleRow1.getString(7))

      	val fullData = List(  fullRow2.getString(0),
                            fullRow2.getString(1),
                            fullRow2.getString(2),
                            fullRow2.getString(3),
                            fullRow2.getString(4),
                            fullRow2.getString(5))

      	DeduplicationPointLabelingContext(List(sampleData,fullData)) 
    	}

    	val groupContext = new DeduplicationGroupLabelingContext("er", Map("fields" -> List("id","entity_id","name","address","city","type")))

    	val f1 = new Feature(Seq(4), Seq(2), Seq("Levenshtein", "CosineSimilarity"))
    	val f2 = new Feature(Seq(5), Seq(3), Seq("Levenshtein", "CosineSimilarity"))
    	val f3 = new Feature(Seq(6), Seq(4), Seq("Levenshtein", "CosineSimilarity"))
    	val f4 = new Feature(Seq(7), Seq(5), Seq("Levenshtein", "CosineSimilarity"))

    	val featureVector = new FeatureVector(Seq(f1,f2,f3,f4))

    	val algoPara = new AlgorithmParameters()
    	algoPara.put("blockingStrategy", BlockingStrategy("Jaccard", 0.6, sampleKey, fullKey))
    	algoPara.put("activeLearningStrategy", ActiveLearningStrategy(featureVector, toPointLabelingContext, groupContext))

    	val d = new Deduplication(algoPara, scc)
    	d.blocking = false
    	d.name = "ActiveLearningDeduplication"

    	val pp = new SampleCleanPipeline(saqp,List(d))
    	pp.exec("restaurant_sample")

  		return ("Dedup", (System.nanoTime - now)/1000000)
  	}
  	else if(command.indexOf("rawsc") >= 0)//todo fix
  	{
  		println(queryParser(command).execute())
  		return ("Complete", (System.nanoTime - now)/1000000)
  	}
  	else if(command.indexOf("watch") >= 0)//todo fix
  	{
  		/*TODO
  		val f = Future{ 
  						var prev:(Long,List[(String, (Double, Double))]) = null
  						var output:(Long,List[(String, (Double, Double))]) = null
  						for(time <- 1 until 20)
    					{ 
      						prev = output
      						output = saqp.rawSCQueryGroup(scc, "restaurant_sample", "id", "count","true","city", 0.1)
      
     						 if(prev != null)
        						{
        							val result = saqp.compareQueryResults(prev,output)
        							if(result._2.length != 0)
        							{
        								println("Change in Query 1: " + result._2.toString())
        							}
        						}

      						Thread.sleep(10000)
    					}
  					  }

  			f.onComplete {
						case Success(value) => println("")
						case Failure(e) => e.printStackTrace
					}*/

  		return ("Complete", (System.nanoTime - now)/1000000)
  	}
  	else
  	{
  		val hiveContext = scc.getHiveContext();
  		hiveContext.hql(command.replace(";","")).collect().foreach(println)
  		return ("Complete", (System.nanoTime - now)/1000000)
  	}

  }

  def initDemo() = {
  	val hiveContext = scc.getHiveContext();
  	hiveContext.hql("DROP TABLE restaurant")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS restaurant (id STRING, entity_id STRING, name STRING, address STRING,city STRING,type STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    hiveContext.hql("LOAD DATA LOCAL INPATH 'restaurant.csv' OVERWRITE INTO TABLE restaurant")
    scc.closeHiveSession()
  }

}