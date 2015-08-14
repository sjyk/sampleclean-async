package sampleclean.parse

import sampleclean.api.{SampleCleanQuery,SampleCleanAQP,SampleCleanContext}
import org.apache.spark.sql.SchemaRDD

/**
 * The SampleCleanQueryParser is the class that handles parsing SampleClean commands.
 * This class triggers execution when a command is parsed successfully. Commands
 * that are not understood are passed down to the HiveContext which executes them
 * as HiveQL Commands.
 *
 * @param scc SampleCleanContext is passed to the parser
 * @param saqp SampleCleanAQP is passed to the parser
 */
@serializable
class SampleCleanQueryParser(scc: SampleCleanContext, saqp:SampleCleanAQP) {

  //descriptive errors during the parse phase
  case class ParseError(val details: String) extends Throwable

   /**
    * Parses a sampleclean query, if not a query returns false
    * @type {[type]}
    */
   def parse(commandI:String):(Boolean, SchemaRDD) = {
  	 
  	  val now = System.nanoTime
  	  val command = commandI.replace(";","").trim()
      val firstToken = command.split("\\s+")(0)
      
      if(firstToken.equals("selectrawsc")){
  		  return (true,queryParser(command).execute())
  	  }
      else if(firstToken.equals("selectnsc")){
        return (true,queryParser(command, false).execute())
      }
      else
      	return (false,null)

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
   									table.trim,
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
}