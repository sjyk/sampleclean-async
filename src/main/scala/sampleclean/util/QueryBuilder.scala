package sampleclean.util

import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanContext._

class QueryBuilder(scc: SampleCleanContext) {
}

object QueryBuilder {

	val CTAS_TEMPLATE = "CREATE TABLE %s AS "
	val HASH_COL_NAME = "hash"
	val DUP_COL_NAME = "dup"
	val HASH_DUP_INIT = "REFLECT(\"java.util.UUID\", \"randomUUID\") AS %h, 1 AS %d,"
	val SAMPLE_TEMPLATE = "TABLESAMPLE( %r PERCENT)"

	def createTableAsSelect(tableName:String): String ={
		return CTAS_TEMPLATE.replace("%s",tableName)
	}

	def tableSample(sampleRatio:Double):String ={
		return SAMPLE_TEMPLATE.replace("%r",sampleRatio+"")
	}

	def attrsToSelectionList(attrs:List[String]): String = {
		var selectionString = ""
		for (attr <- attrs)
		{
			if (selectionString == "")
				selectionString = attr
			else
				selectionString = selectionString + "," + attr

		}
		return selectionString
	}

	def buildSelectQuery(attrs:List[String],table:String,pred:String):String = {
		if (pred == "")
			return buildSelectQuery(attrs,table)
		else
			return "SELECT " + attrsToSelectionList(attrs) + " FROM " + table + " WHERE " + pred  
	}

	def buildSelectQuery(attrs:List[String],table:String,pred:String,table2:String,joinKey:String):String = {
		 val query =    " SELECT "+ 
   			            attrsToSelectionList(attrs) +" FROM "+
   			            table+" LEFT OUTER JOIN " + 
   			            table2+" ON ("+
   			            table+"." + joinKey+ 
   			            " = "+table2+"."+joinKey+")" +
						" WHERE " + pred
		return query  
	}

	def buildSelectQuery(attrs:List[String],table:String):String = {
		return "SELECT " + attrsToSelectionList(attrs) + " FROM " + table 
	}

	def predicateToCase(pred:String):String = {
		return "if((" + pred + "),1.0,0.0)"
	}

	def predicateToCaseMult(pred:String, attr:String):String = {
		return attr+"*"+predicateToCase(pred)
	}

	def getCleanSampleName(sampleName:String):String = {
		return sampleName + "_clean"
	}

	def getDirtySampleName(sampleName:String):String = {
		return sampleName + "_dirty"
	}

	def divide(a:String, b:String):String = {
		return a + "/" + b
	}

	def subtract(a:String, b:String):String = {
		return a + " - " + b
	}

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

	def parenthesize(expr:String):String = {
		return "(" + expr + ")"
	}

}