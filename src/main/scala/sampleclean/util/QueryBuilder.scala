package sampleclean.util

import sampleclean.api.SampleCleanContext

/**
 * The QueryBuilder Class provides a set
 * of methods to manipulate HIVEQL queries.
 */
@serializable
private [sampleclean] class QueryBuilder(scc: SampleCleanContext) {

	//some common templates
	private [sampleclean] val CTAS_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s AS "
  private [sampleclean] val CTASC_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s COMMENT '%b' AS "
  private [sampleclean] val HASH_COL_NAME = "hash"
  private [sampleclean] val DUP_COL_NAME = "dup"
  private [sampleclean] val HASH_DUP_INIT = " REFLECT(\"java.util.UUID\", \"randomUUID\") AS %h, 1 AS %d, "
  private [sampleclean] val SAMPLE_TEMPLATE = " TABLESAMPLE( %r PERCENT)"

	/** Returns the string corresponding to the 
	* CTAS query
	*/
  private [sampleclean] def createTableAs(tableName:String): String ={
		return CTAS_TEMPLATE.replace("%s",tableName)
	}

  private [sampleclean] def setTableParent(tableName:String,baseTable:String): String = {
		return "ALTER TABLE "+tableName + " SET TBLPROPERTIES ('comment' = '"+baseTable+"')"
	}

	/** Returns the string corresponding to the 
	* Insert Overwrite query
	*/
  private [sampleclean] def overwriteTable(tableName:String): String ={
		return "INSERT OVERWRITE TABLE "+ tableName + " "
	}

	/** Returns the syntax for table sampling
	*/
  private [sampleclean] def tableSample(sampleRatio:Double):String ={
		return SAMPLE_TEMPLATE.replace("%r",sampleRatio*100+"")
	}

	/** Returns the syntax for table sampling
	*/
  private [sampleclean] def tableConsistentHash(sampleFrac:Long, onKey:String):String ={
		return " where hash(" +onKey +") % " + sampleFrac + " = 0" 
	}

	/** Takes a list of attributes and formats them into a selection string
	*/
  private [sampleclean] def attrsToSelectionList(attrs:List[String]): String = {
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

	/** This builds a select distinct query with an attribute list, a table, and a predicate
	* if the predicate is blank there is no where clause.
	*/
	def buildSelectDistinctQuery(attrs:List[String],table:String,pred:String):String = {
		if (pred == "")
			return buildSelectQuery(attrs,table)
		else
			return "SELECT DISTINCT " + attrsToSelectionList(attrs) + " FROM " + table + " WHERE " + pred  
	}

	/** This builds a select query that joins with a larger table. Same syntax as above just specifying
	* an additional table and join key
	*/
	def buildSelectQuery(attrs:List[String],table:String,pred:String,table2:String,joinKey:String):String = {
		 val query =    " SELECT " + forceMapJoin(table,table2) +
   			            attrsToSelectionList(attrs) +" FROM "+
   			            table+" LEFT OUTER JOIN " + 
   			            table2+" ON ("+
   			            table+"." + joinKey+ 
   			            " = "+table2+"."+joinKey+")" +
						" WHERE " + pred
		return query  
	}

		/** This builds a select query that joins with a larger table. Same syntax as above just specifying
	* an additional table and join key
	*/
	def buildSelectQuery(attrs:List[String],table:String,pred:String,table2:String,joinKey:String,joinKey2:String, dirty:Boolean =false):String = {
		 val query =    " SELECT " + forceMapJoin(table,table2) +
   			            attrsToSelectionList(attrs) +" FROM "+
   			            table+" LEFT OUTER JOIN " + 
   			            table2+" ON ("+
   			            formatJoinKey(joinKey,table,dirty) +
   			            " = "+formatJoinKey(joinKey2,table2,dirty)+")" +
						" WHERE " + pred
		return query  
	}

  private [sampleclean] def formatJoinKey(joinKey:String, table:String, dirty:Boolean):String = {
		if(joinKey.indexOf(".") < 0){
			return table+"." + joinKey
		}
		else{
				var suffix = "_dirty"
				if(!dirty)
					suffix = "_clean"

				return joinKey.replace(".",suffix+".")
		}
	}

  private [sampleclean] def forceMapJoin(table1:String, table2:String):String = {
		return "/*+ MAPJOIN("+table1+"), MAPJOIN("+table2+") */"
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
  private [sampleclean] def predicateToCase(pred:String):String = {
		return "if((" + pred + "),1.0,0.0)"
	}

	/** Often we have to convert predicates into case statements,
	* and multiply by an attribute
	*/
  private [sampleclean] def predicateToCaseMult(pred:String, attr:String):String = {
		return attr+"*"+predicateToCase(pred)
	}

  private [sampleclean] def addColsToTable(cols:List[String], tableName:String):String ={
		var alterTables = "ALTER TABLE "+ tableName +  " ADD COLUMNS ("
		for(col <- cols)
			alterTables += col + " string, "

		alterTables = alterTables.substring(0,alterTables.length - 2)
			
		alterTables += ")"

		return alterTables
	}

	/** Returns the "clean" sample name
	*/
	def getCleanSampleName(sampleName:String):String = {
		val sampleExprSplit = sampleName.replaceAll("=", " = ")
		val splitComponents = sampleExprSplit.split("\\s+")
		val reservedWords = List("on", "join", "left", "right", "outer", "semi", "=")
		var resultString = ""
		for(comp <- splitComponents){
			if(reservedWords contains comp){
				resultString = resultString + " "+ comp
			}
			else{
				
				if(comp.indexOf(".") >= 0){
					resultString = resultString + " "+ 
					                            comp.substring(0, comp.indexOf("."))+
												"_clean" +
												comp.substring(comp.indexOf("."))
				}
				else{
					resultString = resultString + " "+ comp+"_clean"
				}
			}
		}
		return resultString
	}

	/** Returns the "clean" sample name
	*/
	def getBaseName(sampleName:String):String = {
		val sampleExprSplit = sampleName.replaceAll("=", " = ")
		val splitComponents = sampleExprSplit.split("\\s+")
		val reservedWords = List("on", "join", "left", "right", "outer", "semi", "=")
		var resultString = ""
		for(comp <- splitComponents){
			if(reservedWords contains comp){
				resultString = resultString + " "+ comp
			}
			else{
				
				if(comp.indexOf(".") >= 0){
					resultString = resultString + " "+ 
					                            comp.substring(0, comp.indexOf("."))+
												"_base" +
												comp.substring(comp.indexOf("."))
				}
				else{
					resultString = resultString + " "+ comp+"_base"
				}
			}
		}
		return resultString
	}

  private [sampleclean] def getCleanFactSampleName(sampleName:String,dirty:Boolean = false):String = {
		var suffix = "_clean"

		if(dirty)
			suffix = "_dirty"

		val splitComponents = sampleName.trim().split("\\s+")
		return splitComponents(0)+suffix
	}

  private [sampleclean] def getFactSampleName(sampleName:String):String = {
		val splitComponents = sampleName.split("\\s+")
		return splitComponents(0)
	}

	def joinExpr(sampleName:String):(String,String,String,String) ={
		val splitComponents = sampleName.replace("=", " = ").split("\\s+")

		return(splitComponents(0),splitComponents(2),splitComponents(4),splitComponents(6))
	}

  private [sampleclean] def getTableJoinSchemaList(table1:String,table2:String):List[String] ={
		val schema1 = scc.getHiveTableSchema(table1).map(concatTableName(_,table1))
		var schema2 = scc.getHiveTableSchema(table2).map(concatTableName(_,table2))
		schema2 = schema2.slice(2,schema2.size)
		return schema1 ::: schema2
	}

  private [sampleclean] def concatTableName(attr:String,tableName:String):String ={
		return tableName + "." + attr
	}

  private [sampleclean] def getCleanDimSampleName(sampleName:String,dirty:Boolean = false):String = {
		var suffix = "_clean"

		if(dirty)
			suffix = "_dirty"

		val splitComponents = sampleName.split("\\s+")
		return splitComponents(2)+suffix
	}

  private [sampleclean] def getDimSampleName(sampleName:String):String = {
		val splitComponents = sampleName.split("\\s+")
		return splitComponents(2)
	}

  private [sampleclean] def transformExplicitExpr(expr:String, sampleName:String, dirty:Boolean=false):String = {
		val splitComponents = sampleName.split("\\s+")
		var result = expr
		if(splitComponents.length > 1){
			result = result.replaceAll(getDimSampleName(sampleName),
									 getCleanDimSampleName(sampleName, dirty))

			result = result.replaceAll(getFactSampleName(sampleName),
									 getCleanFactSampleName(sampleName,dirty))
		}
		else
		{
			result = result.replaceAll(getFactSampleName(sampleName),
									 getCleanFactSampleName(sampleName,dirty))
		}

		return result
	}

  private [sampleclean] def isJoinQuery(sampleName:String):Boolean = {
		val splitComponents = sampleName.split("\\s+")
		return (splitComponents.length > 1)
	}

  private [sampleclean] def exprToDupString(sampleName:String):String = {
		val splitComponents = sampleName.split("\\s+")
		if(splitComponents.length > 1){
			return getCleanFactSampleName(sampleName) + ".dup"	//+ 
				  //getCleanDimSampleName(sampleName) + ".dup"
		}
		else{
			return "dup"
		}
	}

  private [sampleclean] def countSumVarianceSQL(k: Double, attr: String, sampleRatio: Double):String = {
		val realCount = "sum("+predicateToCase("agg != 0")+")" //todo fix

		val n = "("+k/sampleRatio+")"
		val p = realCount +"/" + k
		val ps = p + " * (1.0 - " + p + ")"
		val sf = k + "/" + realCount

		val means = "sum(agg)/"+realCount
		val meansS = "("+means+")*(" + means+")"
		val stds = "var_samp("+attr+")*"+sf
		val firstTerm = ps+"*"+n+"*"+n+"*"+meansS
		val secondTerm = p+"*"+n+"*"+n+"*"+stds

		return "("+firstTerm+"+"+secondTerm+")/"+k
	}

	/** Returns the "dirty" sample name
	*/
	def getDirtySampleName(sampleName:String):String = {
		val sampleExprSplit = sampleName.replaceAll("=", " = ")
		val splitComponents = sampleExprSplit.split("\\s+")
		val reservedWords = List("on", "join", "left", "right", "outer", "semi", "=")
		var resultString = ""
		for(comp <- splitComponents){
			if(reservedWords contains comp){
				resultString = resultString + " "+ comp
			}
			else{
				
				if(comp.indexOf(".") >= 0){
					resultString = resultString + " "+
					                            comp.substring(0, comp.indexOf("."))+
												"_dirty" +
												comp.substring(comp.indexOf("."))
				}
				else{
					resultString = resultString + " "+ comp+"_dirty"
				}
			}
		}

		return resultString
	}

	/** Divide two attributes
	*/
  private [sampleclean] def divide(a:String, b:String):String = {
		return a + "/" + b
	}

	/** Multiply two attributes
	*/
  private [sampleclean] def subtract(a:String, b:String):String = {
		return a + " - " + b
	}

	/** This method seperates an experssion on 
	* punctutation. This allows us to parse the
	*expression more easily.
	*/
  private [sampleclean] def punctuationParseString(expr:String):String = {
		var resultString = " "
		for (i <- 0 until expr.length)
		{
			if(expr(i).isLetterOrDigit || expr(i) == '_')
				resultString = resultString + expr(i)
			else
				resultString = resultString + ' ' + expr(i) + ' '
		}

		return resultString + " "
	}

	/** This makes the experssion also contain the table name
	* in the form table.attr rather than just attr.
	*/
  private [sampleclean] def makeExpressionExplicit(expr:String,
		                       sampleExpName:String):String = {

		var resultExpr = " "+punctuationParseString(expr.toLowerCase)
		for(col <- scc.getHiveTableSchema(sampleExpName))
		{
			resultExpr = resultExpr.replaceAll(' ' + col.toLowerCase + ' ', ' ' + sampleExpName+'.'+col + ' ')
		}

		return resultExpr
	}

	/** puts the expression in parens
	*/
  private [sampleclean] def parenthesize(expr:String):String = {
		return "(" + expr + ")"
	}

  private [sampleclean] def appendToPredicate(pred:String, expr:String): String = {
		return parenthesize(pred) + " AND " + parenthesize(expr)
	}

  private [sampleclean] def attrEquals(attr:String, value:String):String = {
		return attr + " = '" + value+"'"
	}

}