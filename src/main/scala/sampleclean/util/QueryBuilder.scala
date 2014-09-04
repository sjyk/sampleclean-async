package sampleclean.util

import sampleclean.api.SampleCleanContext;
import sampleclean.api.SampleCleanContext._

/** The QueryBuilder Class provides a set 
* of methods to manipulate HIVEQL queries.
*/
class QueryBuilder(scc: SampleCleanContext) {
}

object QueryBuilder {

	//some common templates
	val CTAS_TEMPLATE = "CREATE TABLE %s AS "
	val CTASC_TEMPLATE = "CREATE TABLE %s COMMENT '%b' AS "
	val HASH_COL_NAME = "hash"
	val DUP_COL_NAME = "dup"
	val HASH_DUP_INIT = " REFLECT(\"java.util.UUID\", \"randomUUID\") AS %h, 1 AS %d, "
	val SAMPLE_TEMPLATE = " TABLESAMPLE( %r PERCENT)"

	/** Returns the string corresponding to the 
	* CTAS query
	*/
	def createTableAs(tableName:String): String ={
		return CTAS_TEMPLATE.replace("%s",tableName)
	}

	def setTableParent(tableName:String,baseTable:String): String = {
		return "ALTER TABLE "+tableName + " SET TBLPROPERTIES ('comment' = '"+baseTable+"')"
	}

	/** Returns the string corresponding to the 
	* Insert Overwrite query
	*/
	def overwriteTable(tableName:String): String ={
		return "INSERT OVERWRITE TABLE "+ tableName + " "
	}

	/** Returns the syntax for table sampling
	*/
	def tableSample(sampleRatio:Double):String ={
		return SAMPLE_TEMPLATE.replace("%r",sampleRatio+"")
	}

	/** Takes a list of attributes and formats them into a selection string
	*/
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

	/** This builds a select query with an attribute list, a table, and a predicate
	* if the predicate is blank there is no where clause.
	*/
	def buildSelectQuery(attrs:List[String],table:String,pred:String):String = {
		if (pred == "")
			return buildSelectQuery(attrs,table)
		else
			return "SELECT " + attrsToSelectionList(attrs) + " FROM " + table + " WHERE " + pred  
	}

	/** This builds a select query that joins with a larger table. Same syntax as above just specifying
	* an additional table and join key
	*/
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

	/** This builds a select query with a semi join with another table, that is, keep only records in the other table
	indexed by key.
	*/
	def buildSelectSemiJoinQuery(attrs:List[String],table:String,pred:String,table2:String,joinKey:String):String = {
		 val query =    " SELECT "+ 
   			            attrsToSelectionList(attrs) +" FROM "+
   			            table+" JOIN " + 
   			            table2+" ON ("+
   			            table+"." + joinKey+ 
   			            " = "+table2+"."+joinKey+")" +
						" WHERE " + pred
		return query  
	}

	/** This builds a select query with an attribute list, a table, and a predicate
	* if the predicate is blank there is no where clause.
	*/
	def buildSelectQuery(attrs:List[String],table:String):String = {
		return "SELECT " + attrsToSelectionList(attrs) + " FROM " + table 
	}

	/** Often we have to convert predicates into case statement
	*/
	def predicateToCase(pred:String):String = {
		return "if((" + pred + "),1.0,0.0)"
	}

	/** Often we have to convert predicates into case statements,
	* and multiply by an attribute
	*/
	def predicateToCaseMult(pred:String, attr:String):String = {
		return attr+"*"+predicateToCase(pred)
	}

	/** Returns the "clean" sample name
	*/
	def getCleanSampleName(sampleName:String):String = {
		return sampleName + "_clean"
	}

	/** Returns the "dirty" sample name
	*/
	def getDirtySampleName(sampleName:String):String = {
		return sampleName + "_dirty"
	}

	/** Divide two attributes
	*/
	def divide(a:String, b:String):String = {
		return a + "/" + b
	}

	/** Multiply two attributes
	*/
	def subtract(a:String, b:String):String = {
		return a + " - " + b
	}

	/** This method seperates an experssion on 
	* punctutation. This allows us to parse the
	*expression more easily.
	*/
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

	/** This makes the experssion also contain the table name
	* in the form table.attr rather than just attr.
	*/
	def makeExpressionExplicit(expr:String, 
		                       sampleExpName:String):String = {

		var resultExpr = punctuationParseString(expr.toLowerCase)
		for(col <- getHiveTableSchema(sampleExpName))
		{
			resultExpr = resultExpr.replaceAll(' ' + col.toLowerCase + ' ', ' ' + sampleExpName+'.'+col + ' ')
		}

		return resultExpr
	}

	/** puts the expression in parens
	*/
	def parenthesize(expr:String):String = {
		return "(" + expr + ")"
	}

}