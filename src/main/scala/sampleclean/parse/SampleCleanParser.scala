package sampleclean.parse

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanContext._

/* This class parses a command 
*  it takes an SC command argument
*  and tokenizes it.
*/
class SampleCleanParser(scc: SampleCleanContext) {

  /* This command returns a result and a code tuple.
   */
  def parseAndExecute(command:String):(String, Int) = {

  	System.out.println(command);

  	return ("", 0)

  }

}

object SampleCleanParser {

	def punctuationParseString(expr:String):String = {
		var resultString = " "
		for (i <- 0 until expr.length)
		{
			if(expr(i).isLetterOrDigit)
				resultString = resultString + expr(i)
			else
				resultString = resultString + ' ' + expr(i) + ' '
		}

		return resultString + " "
	}

	def makeExpressionExplicit(expr:String, 
		                       sampleExpName:String):String = {

		var resultExpr = punctuationParseString(expr.toLowerCase)
		for(col <- getHiveTableSchema(sampleExpName))
		{
			resultExpr = resultExpr.replaceAll(' ' + col.toLowerCase + ' ', ' ' + sampleExpName+'.'+col + ' ')
		}

		return resultExpr
	}

}