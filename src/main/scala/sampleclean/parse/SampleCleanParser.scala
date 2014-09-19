package sampleclean.parse

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, Row}

import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanAQP;
import sampleclean.api.SampleCleanQuery;

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import sampleclean.clean.outlier.ParametricOutlier;
import sampleclean.clean.deduplication._
import sampleclean.activeml._
import sampleclean.clean.algorithm.AlgorithmParameters
import sampleclean.clean.algorithm.SampleCleanAlgorithm
import sampleclean.clean.algorithm.SampleCleanPipeline

import sampleclean.clean.deduplication.WordTokenizer
import sampleclean.clean.deduplication.BlockingStrategy
import sampleclean.clean.deduplication.BlockingKey
import sampleclean.clean.deduplication.ActiveLearningStrategy

/** The SampleCleanParser is the class that handles parsing SampleClean commands
 *  this class triggers execution when a command is parsed successfully. Commands
 *  that are not understood are passed down to the HiveContext which executes them
 *  as HiveQL Commands.
 *
 * @param scc SampleCleanContext is passed to the parser
 * @param saqp SampleCleanAQP is passed to the parser
 */
@serializable
class SampleCleanParser(scc: SampleCleanContext, saqp:SampleCleanAQP) {

  //descriptive errors during the parse phase
  case class ParseError(val details: String) extends Throwable

  //function registry
  val functionRegistry = Map("merge" -> ("sampleclean.clean.misc.MergeKey",
                                         List("attr","src","target")))

  /**
   * This command parses SampleClean "reserved" command. Basically these commands 
   * are those which are not SQL commands. Eg. merge key1 key2, to support these
   * declarative operations, this function offers a rudimentary tokenizer. 
   *
   * To support mutli-word strings, it tokenizes on string boundaries ie using single 
   * quotes to break words.
   * 
   * @param command a string command
   * @return a list of tokens in order
   */
  def reservedCommandTokenizer(command:String):List[String] = {

  		//split the tokens on whitespace
      val splitComponents = command.split("\\s+")

  		var result = List[String]()
  		var mergeBuffer = ""

      //iterate through tokens
  		for(i <- splitComponents)
  		{
        //is there a string with a single quote
  			val stringSignal = i.indexOf("'")

  			if(mergeBuffer.equals("") &&
  				stringSignal >= 0) //we are not already parsing a string
  			{
  				if(i.indexOf("'",stringSignal+1) >= 0)
  					result = i.replaceAll("'","") :: result
  				else
  					mergeBuffer = mergeBuffer + " " + i
  			}
  			else if(!mergeBuffer.equals("") &&
  				stringSignal >= 0) //we are already parsing a string
  			{
  				mergeBuffer = mergeBuffer + " " + i
  				result = mergeBuffer.replaceAll("'","") :: result
  				mergeBuffer = ""
  			}
  			else {
  				result = i :: result
  			}
  			
  		}
  		return result.reverse //since we use pre-pending operations reverse
  }

   /**
    * This command parses a sampleclean SQL-like query
    * the return object is a SampleCleanQuery object.
    * @param command a String representation of the query. 
    * Expects lower case strings
    */
   def queryParser(command:String):SampleCleanQuery={

   		val from = command.indexOf("from") 
      //every sample clean query should have a from
   		if(from < 0)
   			throw new ParseError("SELECT-type query does not have a from")

   		val where = command.indexOf("where") //predicate 
   		val groupby = command.indexOf("group by") //group by
   		val expr = command.substring(0,from) //select statement
   		
      //the default behavior for missing clauses is an empty string
      //the query execution handles this appropriately
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

   /**
    * The expression parser parses a select statement and figures 
    * out what to do with it. Basically there are two return objects 
    * (operator, attr)
    * @param command lower case expr string
    */
   def expressionParser(command:String):(String, String) ={
   		val splitComponents = command.split("\\s+") //split on whitespace
   		val exprClean = splitComponents.drop(1).mkString(" ") //remove 'select*'
   		val attrStart = exprClean.indexOf("(")
   		val attrEnd = exprClean.indexOf(")")

   		if(attrStart == -1 || attrEnd == -1)
   			throw new ParseError("SampleClean can only estimate properly formatted Aggregate Queries")

   		else{
   			var attr = exprClean.substring(attrStart+1,attrEnd).trim()

   			if(attr == "1") //always set to a col name
   				attr = "hash"

   			return (exprClean.substring(0,attrStart),attr)
   		}
   }

   /**
    * This function initializes the SC Table from the base
    * data.
    * @param command this is a lower case string with the command
    */
   def initSCTables(command:String) ={
      val comps = reservedCommandTokenizer(command)
      
      if(comps.length != 4)
        throw new ParseError("Usage: init <samplename> <bt> <samplingratio>")
      
      val name = comps(1)
      val bt = comps(2)

      val samplingRatio = comps(3).toDouble
      
      scc.initialize(bt,name,samplingRatio,true)
   }

  /** Waring this function destructively removes progress
   *  to reset the state of the system. You will have to
   *  reinit.
   */
  def clearSession() = {
        scc.closeHiveSession()
    }

   /**
    * This uses Java reflections to execute a library routine
    * from the data cleaning library adding the name, the class,
    * and the params to the function registry will allow this to 
    * get executed.
    * 
    * @param command this is a lower case string with the command
    */
   def execLibraryRoutine(command:String) = {
      val comps = reservedCommandTokenizer(command)

      val classData = functionRegistry.get(comps(0)).get

      if(comps.length != classData._2.length + 2)
        throw new ParseError("Usage: <function> <sampleName> [args...]")

      val name = comps(1)
      val algoPara = new AlgorithmParameters()
      var paramIndex = 2

      for(param <- classData._2){
         algoPara.put(param, comps(paramIndex))
         paramIndex = paramIndex + 1
      }
      val d =  Class.forName(classData._1).getConstructors()(0).newInstance(algoPara,scc).asInstanceOf[SampleCleanAlgorithm]
      d.blocking = false
      d.name = classData._1
      val pp = new SampleCleanPipeline(saqp,List(d))
      pp.exec(name)
   }
 
  /**
   * This function parses the command and executes
   * if the command is not understood it executes it
   * as a hive command.
   * @param commandI string typed into the repl
   * @return (String, Long) Log message and a runtime in (ms)
   */
  def parseAndExecute(commandI:String):(String, Long) = {

  	val now = System.nanoTime
  	val command = commandI.replace(";","").toLowerCase().trim()
    val firstToken = command.split("\\s+")(0)

    
	  if(command.equals("load demo")) {
  		initDemo()
  		return ("Demo Initialized", (System.nanoTime - now)/1000000)
  	}
  	else if(firstToken.equals("init")) {
   		initSCTables(command)
  		return("Create SC Table", (System.nanoTime - now)/1000000)
  	}
    else if(firstToken.equals("clear")) {
      clearSession()
      return("Removed Temp Tables", (System.nanoTime - now)/1000000)
    }
  	else if(functionRegistry.contains(firstToken)){
      execLibraryRoutine(command)
  		return ("Lib Routine", (System.nanoTime - now)/1000000)
  	}
  	else if(firstToken.equals("dedup"))
  	{
      demoDedup()
  		return ("Dedup", (System.nanoTime - now)/1000000)
  	}
  	else if(firstToken.equals("selectrawsc"))
  	{
  		println(queryParser(command).execute())
  		return ("Complete", (System.nanoTime - now)/1000000)
  	}
  	else //in the default case pass it to hive
  	{
  		val hiveContext = scc.getHiveContext();
  		hiveContext.hql(command.replace(";","")).collect().foreach(println)
  		return ("Complete", (System.nanoTime - now)/1000000)
  	}

  }


  //Demo functions

  def initDemo() = {
  	val hiveContext = scc.getHiveContext();
  	hiveContext.hql("DROP TABLE restaurant")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS restaurant (id STRING, entity_id STRING, name STRING, address STRING,city STRING,type STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    hiveContext.hql("LOAD DATA LOCAL INPATH 'restaurant.csv' OVERWRITE INTO TABLE restaurant")
    scc.closeHiveSession()
  }

  def demoDedup() = {
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
  }


}