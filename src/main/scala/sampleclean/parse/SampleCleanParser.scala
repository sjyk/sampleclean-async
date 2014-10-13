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
import sampleclean.clean.deduplication.CrowdsourcingStrategy

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

  val RESERVED_STRING_CHAR = "\""

  //function registry--use the function registry if the system accepts string parameters
  val functionRegistry = Map("merge" -> ("sampleclean.clean.misc.MergeKey",
                                         List("attr","src","target")),
                             "lowercase" -> ("sampleclean.clean.misc.LowerCaseTrimKey",
                                         List("attr")),
                             "filter" -> ("sampleclean.clean.misc.RuleFilter",
                                         List("attr", "rule")))

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
        //is there a string
  			val stringSignal = i.indexOf(RESERVED_STRING_CHAR)

  			if(mergeBuffer.equals("") &&
  				stringSignal >= 0) //we are not already parsing a string
  			{
  				if(i.indexOf(RESERVED_STRING_CHAR,stringSignal+1) >= 0)
  					result = i.replaceAll(RESERVED_STRING_CHAR,"") :: result
  				else
  					mergeBuffer = mergeBuffer + " " + i
  			}
  			else if(!mergeBuffer.equals("") &&
  				stringSignal >= 0) //we are already parsing a string and finish
  			{
  				mergeBuffer = mergeBuffer + " " + i
  				result = mergeBuffer.replaceAll(RESERVED_STRING_CHAR,"") :: result
  				mergeBuffer = ""
  			}
        else if(!mergeBuffer.equals("")) //we are already parsing a string
        {
          mergeBuffer = mergeBuffer + " " + i
        }
  			else {
  				result = i :: result
  			}
  			
  		}

      //println(result)
  		
      return result.map(_.trim()).reverse //since we use pre-pending operations reverse
  }

   /**
    * This command parses a sampleclean SQL-like query
    * the return object is a SampleCleanQuery object.
    * @param command a String representation of the query. 
    * Expects lower case strings
    */
   def queryParser(command:String, rawSC:Boolean = true):SampleCleanQuery={

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
   									group,
                    rawSC)
   		
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
        throw new ParseError("Usage: init <samplename> <bt> <unique key> <samplingratio>")
      
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

      println(comps)

      val classData = functionRegistry.get(comps(0)).get

      if(comps.length != classData._2.length + 2)
        throw new ParseError("Usage: <function> <sampleName> [args...]")

      val name = comps(1)
      val algoPara = new AlgorithmParameters()
      var paramIndex = 2

      for(param <- classData._2){
         algoPara.put(param, comps(paramIndex).trim())
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
  	val command = commandI.replace(";","").trim()
    val firstToken = command.split("\\s+")(0)

    if(firstToken.equals(""))
      return ("Error", (System.nanoTime - now)/1000000 )

    try{

     if(firstToken.equals("quit")){
        return ("Quit", (System.nanoTime - now)/1000000)
     }
	   else if(command.equals("load demo")) {
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
  	 else if(firstToken.equals("deduprec")){
        demoDedupRec()
  		  return ("Dedup", (System.nanoTime - now)/1000000)
  	 }
     else if(firstToken.equals("dedupattr")){
       demoDedupAttr()
       return ("Dedup", (System.nanoTime - now)/1000000)
     }
  	 else if(firstToken.equals("selectrawsc")){
  		  printQuery(queryParser(command).execute())
  		  return ("Complete", (System.nanoTime - now)/1000000)
  	  }
      else if(firstToken.equals("selectnsc")){
        printQuery(queryParser(command, false).execute())
        return ("Complete", (System.nanoTime - now)/1000000)
      }
  	 else {//in the default case pass it to hive
  		  val hiveContext = scc.getHiveContext();
  		  hiveContext.hql(command.replace(";","")).collect().foreach(println)
  		  return ("Complete", (System.nanoTime - now)/1000000)
  	  }

    }
    catch {
     case p: ParseError => println(p.details)
     case e: Exception => println(e)
   }

    return ("Error", (System.nanoTime - now)/1000000 )

  }

  def printQuery(result:(Long, List[(String, (Double, Double))])) ={
      var listOfResults = result._2
      listOfResults = listOfResults.sortBy(-_._2._1)
      for(r <- listOfResults.slice(0,Math.min(10, listOfResults.length)))
        println(r)
  }

  //Demo functions

  def initDemo() = {
    val hiveContext = scc.getHiveContext();
    scc.closeHiveSession()
  	hiveContext.hql("DROP TABLE IF EXISTS paper")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS paper(id String,title string,year String,conference String,journal String,keyword String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    hiveContext.hql("LOAD DATA LOCAL INPATH 'msac-datasets/Paper-reg.csv' OVERWRITE INTO TABLE paper")

    hiveContext.hql("DROP TABLE IF EXISTS paper_author")
    hiveContext.hql("CREATE TABLE IF NOT EXISTS paper_author(paperId String,authorId String,Name String,Affiliation String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    hiveContext.hql("LOAD DATA LOCAL INPATH 'msac-datasets/PaperAuthor-reg.csv' OVERWRITE INTO TABLE paper_author")

    hiveContext.hql("DROP TABLE IF EXISTS paper_affiliation")
    hiveContext.hql("CREATE TABLE paper_affiliation as SELECT paperid, affiliation from paper_author where length(affiliation) > 1 group by paperid,affiliation")
  
    scc.initializeConsistent("paper", "paper_sample", "id", 50)
    scc.initializeConsistent("paper_affiliation", "paper_aff_sample", "paperid", 50)
    scc.initializeConsistent("paper_author", "paper_auth_sample", "paperid", 50)

  }

  def demoDedupRec() = {

    val algoPara = new AlgorithmParameters()

    algoPara.put("id","id")

    val blockedCols = List("name", "address", "city", "type")
    algoPara.put("blockingStrategy", BlockingStrategy(blockedCols).setThreshold(0.4))

    val displayedCols = List("id","entity_id", "name", "address", "city", "type")
    var featureList = List[Feature]()
    featureList = Feature(List("name"), List("Levenshtein", "JaroWinkler")) :: featureList
    featureList = Feature(List("address"), List("JaccardSimilarity", "QGramsDistance")) :: featureList
    featureList = Feature(List("city"), List("Levenshtein", "JaroWinkler")) :: featureList
    featureList = Feature(List("type"), List("Levenshtein", "JaroWinkler")) :: featureList

    algoPara.put("activeLearningStrategy",
      ActiveLearningStrategy(displayedCols)
        .setFeatureList(featureList)
        .setActiveLearningParameters(ActiveLearningParameters(budget = 60, batchSize = 10, bootstrapSize = 10)))

    val d = new RecordDeduplication(algoPara, scc)
    d.blocking = false
    d.name = "ActiveLearningDeduplication"

    val pp = new SampleCleanPipeline(saqp, List(d))
    pp.exec("restaurant_sample")
  }


  def demoDedupAttr() = {

    val algoPara = new AlgorithmParameters()

    algoPara.put("dedupAttr", "affiliation")
    algoPara.put("similarityParameters", SimilarityParameters(bitSize=5))


    val crowdParameters = CrowdLabelGetterParameters(maxPointsPerHIT = 10)
    //algoPara.put("crowdsourcingStrategy", CrowdsourcingStrategy().setCrowdLabelGetterParameters(crowdParameters))
    val d = new AttributeDeduplication(algoPara, scc)
    d.blocking = false
    d.name = "AttributeDeduplication"

    val pp = new SampleCleanPipeline(saqp, List(d))
    pp.exec("paper_aff_sample")
  }




}