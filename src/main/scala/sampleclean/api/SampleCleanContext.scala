package sampleclean.api

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import scala.util.Random

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import SampleCleanContext._
import sampleclean.util.TypeUtils._
import sampleclean.util.QueryBuilder
import sampleclean.parse.SampleCleanQueryParser

import scala.concurrent._

/**
 * As an analog to the SparkContext, the SampleCleanContext
 * gives a handle to the current session. This class provides
 * the basic API to manipulate the data structures. We assume
 * that the data is initially in a HIVE store.
 *
 * In its current implementation, the SampleCleanContext
 * supports both persistent data in HIVE or keeping the data
 * in memory as an RDD.
 * @param sc an existing Spark Context
 */
@serializable
class SampleCleanContext(@transient sc: SparkContext) {

	//Use these functions to access the Spark
	//and Hive contexts in API calls.

	/**
   * Returns the HiveContext
	 */
	def getHiveContext():HiveContext = {
		return new HiveContext(sc)
	}

	/**
   * Returns the SparkContext
	 */
  private [sampleclean] def getSparkContext():SparkContext = {
		return sc
	}

	//a reference to the contexts Query Builder
  private [sampleclean] val qb = new QueryBuilder(this)

  /**
   * This function initializes the clean and dirty samples as
   * Schema RDD's and returns a tuple (Clean, Dirty). There is an additional
   * flag to persist the RDD in HIVE if desired.
   *
   * @param baseTable name of base table
   * @param sampleTable name of sample table
   * @param samplingRatio sampling ratio between 0.0 and 1.0
   * @param persist set to true to persist RDD in HIVE (default = true)
   */
	def initialize(baseTable:String, sampleTable:String, samplingRatio: Double=1.0, persist:Boolean=true): (SchemaRDD, SchemaRDD) = {

		val hiveContext = new HiveContext(sc)
		//creates the clean sample using table sampling procedure
		//when databricks gives us a better implementation of sampling
		//we can use that. 
		//val selectionList = List("reflect(\"java.util.UUID\", \"randomUUID\") as hash",
		//	                     "1 as dup", "*")
		val selectionList = List("reflect(\"java.util.UUID\", \"randomUUID\") as hash",
			                     "1 as dup", "*")

		if(persist)
		{
			var baseQuery = qb.createTableAs(qb.getBaseName(baseTable)) +
			qb.buildSelectQuery(selectionList,baseTable)

			hiveContext.sql(baseQuery)

			var query = qb.createTableAs(qb.getCleanSampleName(sampleTable)) +
			qb.buildSelectQuery(List("*"),qb.getBaseName(baseTable)) +
			qb.tableSample(samplingRatio)

			hiveContext.sql(query)

			//hiveContext.sql("cache table "+ qb.getCleanSampleName(sampleTable))
			hiveContext.cacheTable(qb.getCleanSampleName(sampleTable))

			hiveContext.sql(qb.setTableParent(qb.getCleanSampleName(sampleTable),qb.getBaseName(baseTable) + " " + samplingRatio))

			query = qb.createTableAs(qb.getDirtySampleName(sampleTable)) +
					qb.buildSelectQuery(List("*"),qb.getCleanSampleName(sampleTable))

			hiveContext.sql(query)

			hiveContext.sql(qb.setTableParent(qb.getDirtySampleName(sampleTable),qb.getBaseName(baseTable) + " " + samplingRatio))

			//hiveContext.sql("cache table "+ qb.getDirtySampleName(sampleTable))
		}
		
		return (hiveContext.sql(qb.buildSelectQuery(List("*"),
								qb.getCleanSampleName(sampleTable))),
				hiveContext.sql(qb.buildSelectQuery(List("*"),
								qb.getDirtySampleName(sampleTable))))
	}


	/**
   * This function initializes the clean and dirty consistently hashed
	 * samples as Schema RDD's in a tuple (Clean, Dirty). 
   * There is an additional flag to persist the rdd in HIVE if desired.
   *
   * @param baseTable name of base table
   * @param sampleTable name of sample table
   * @param onKey name of column that contains unique identifiers
   * @param samplingRatio sampling ratio between 0.0 and 1.0
   * @param persist set to true to persist RDD in HIVE (default = true)
   */
	def initializeConsistent(baseTable:String, sampleTable:String, onKey:String, samplingRatio: Double = 1.0, persist:Boolean=true): (SchemaRDD, SchemaRDD) = {

		val hiveContext = new HiveContext(sc)
		//creates the clean sample using a consistent hashing procedure
		val selectionList = List("reflect(\"java.util.UUID\", \"randomUUID\") as hash",
			                     "1 as dup", "*")
		//val selectionList = List("concat(*)",
		//	                     "1 as dup", "*")

		if(persist)
		{
			var baseQuery = qb.createTableAs(qb.getBaseName(baseTable)) +
			qb.buildSelectQuery(selectionList,baseTable)

			hiveContext.sql(baseQuery)

			var query = qb.createTableAs(qb.getCleanSampleName(sampleTable)) +
			qb.buildSelectQuery(List("*"),qb.getBaseName(baseTable)) +
			qb.tableConsistentHash((1.0 / samplingRatio).toLong,onKey)

			hiveContext.sql(query)

			hiveContext.cacheTable(qb.getCleanSampleName(sampleTable))

			hiveContext.sql(qb.setTableParent(qb.getCleanSampleName(sampleTable),qb.getBaseName(baseTable) + " " + samplingRatio))

			query = qb.createTableAs(qb.getDirtySampleName(sampleTable)) +
					qb.buildSelectQuery(List("*"),qb.getCleanSampleName(sampleTable))
		

			hiveContext.sql(query)

			hiveContext.sql(qb.setTableParent(qb.getDirtySampleName(sampleTable),qb.getBaseName(baseTable) + " " + samplingRatio))
			
		}
		
		return (hiveContext.sql(qb.buildSelectQuery(List("*"),
								qb.getCleanSampleName(sampleTable))), 
				hiveContext.sql(qb.buildSelectQuery(List("*"),
								qb.getDirtySampleName(sampleTable))))
	}
  
  /**
   * This function cleans up after using initializeHive by dropping
   * any temp tables in HIVE. If you use HIVE and don't execute this command, 
   * the samples will persist between sessions as it will be written
   * to disk.
   */
	def closeHiveSession() = {
		val hiveContext = new HiveContext(sc)

		for (t <- getAllTempTables())
		{
			hiveContext.sql("DROP TABLE IF EXISTS " + t)
		}
	}

	/**
	 * This function executes a hiveql query (potentially re-writing it to sampleclean)
	 * @type {Object} result
	 */
	def hql(query:String):SchemaRDD = {
		val parser = new SampleCleanQueryParser(this, new SampleCleanAQP())
		val hiveContext = getHiveContext()
		val result = parser.parse(query)
		
		if(result._1)
			return result._2
		else
			return hiveContext.sql(query)
	}

	/* Returns an RDD which points to a full table
	*/
  private [sampleclean] def getFullTable(sampleName: String):SchemaRDD = {

		val hiveContext = new HiveContext(sc)

		//println(getParentTable(qb.getCleanSampleName(sampleName)))

		return hiveContext.sql(qb.buildSelectQuery(List("*"),
							   getParentTable(qb.getCleanSampleName(sampleName))))
	}
  
  /**
   * Returns an RDD which points to a sample 
   * of a base table.
   * @param sampleTable sample table name
   */
	def getCleanSample(sampleTable: String):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		return hiveContext.sql(qb.buildSelectQuery(List("*"),qb.getCleanSampleName(sampleTable)))
	}

  /**
   * Returns an RDD which points to a sample
   * of a full table. This method returns a tuple
   * of the hash and the requested attribute.
   * @param tableName sample table name
   * @param attr attribute name
   */
  private [sampleclean] def getCleanSampleAttr(tableName: String, attr: String):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		return hiveContext.sql(qb.buildSelectQuery(List("hash",attr),qb.getCleanSampleName(tableName)))
	}

	/* Returns an RDD which points to a sample 
	 * of a full table. This method returns a tuple
	 * of the hash and the requested attribute
	 */
  private [sampleclean] def getCleanSampleAttr(tableName: String, attr: String, pred:String):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		return hiveContext.sql(qb.buildSelectQuery(List("hash",attr),qb.getCleanSampleName(tableName), pred))
	}

	/**
   * Saves the clean sample as a dirty sample
	 */
  private [sampleclean] def rebaseSample(tableName: String) = {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = qb.getCleanSampleName(tableName)
		val tableNameDirty = qb.getDirtySampleName(tableName)
		hiveContext.sql(qb.overwriteTable(tableNameDirty) +
						qb.buildSelectQuery(List("*"),
   		    					            tableNameClean))
	}

  	/**
     * Given a working set this function applies the changes back
     * to the parent table.
	 */
   def writeToParent(sampleName: String) = {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		var parent = getParentTable(qb.getCleanSampleName(sampleName))
		parent = parent.substring(0,parent.length - 5)
		val tableNameClean = qb.getCleanSampleName(sampleName)
		val m = getSamplingRatio(tableNameClean)
		
		if (m < 1.0)
			throw new RuntimeException("Writing back samples is not currently supported")

		hiveContext.sql(qb.overwriteTable(parent) +
						qb.buildSelectQuery(getHiveTableSchema(parent),
   		    					            tableNameClean))
	}

	/**
   * Resets the clean sample to the initial dirty sample
   * @param sampleTable sample table name
	 */
	def resetSample(sampleTable: String) = {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = qb.getCleanSampleName(sampleTable)
		val tableNameDirty = qb.getDirtySampleName(sampleTable)
		hiveContext.sql(qb.overwriteTable(tableNameClean) +
						qb.buildSelectQuery(List("*"),
   		    					            tableNameDirty))
	}

	/**
   * This function takes a sample and a rdd of (Hash, Val) and updates those records in the RDD.
	 * it returns a new updated SchemaRDD, and there is a persist flag to write these results to HIVE.
	 */
  private [sampleclean] def updateTableAttrValue(tableName: String, attr:String, rdd:RDD[(String, String)], persist:Boolean = true): SchemaRDD= {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = qb.getCleanSampleName(tableName)
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		println("[SampleCleanContext]: Update Table Attr Value")

    // TODO spark issues in test
    // 
    	//hql("DROP TABLE tmp")
    	hiveContext.createDataFrame(enforceSSSchema(rdd)).saveAsTable(tmpTableName)
		//hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceSSSchema(rdd)),"tmp")
		//hiveContext.sql(qb.createTableAs(tmpTableName) +qb.buildSelectQuery(List("*"),"tmp"))

		//Uses the hive API to get the schema of the table.
		var selectionString = List[String]()
		var selectionString2 = List[String]()

		for (field <- getHiveTableSchema(tableNameClean))
		{
			if (field.equals(attr))
				selectionString = tmpTableName+"."+"updateAttr as " + attr:: selectionString // To Sanjay: It was tmpNameClean before. Since it failed to compile, I changed it to tableNameClean
			else
				selectionString = tableNameClean+"."+field :: selectionString

			selectionString2 = tableNameClean+"."+field :: selectionString2
		}

		selectionString = selectionString.reverse

   		//applies hive query to update the data
   		if(persist){
   			val tmpTableName2 = tmpTableName+"2"

   			hiveContext.sql(qb.createTableAs(tmpTableName2) +
   		    				qb.buildSelectQuery(selectionString,
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))

   			hiveContext.sql("set hive.optimize.bucketmapjoin = true");
   			hiveContext.sql("drop table "+tableNameClean);

   			hiveContext.sql("ALTER TABLE " + tmpTableName2 + " RENAME TO " +tableNameClean);
   		    /*hiveContext.sql(qb.createTableAs(tmpTableName2) +
   		    				qb.buildSelectQuery(selectionString,
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))*/
   	   }

   		/*return hiveContext.sql(qb.buildSelectQuery(selectionString, 
   			                   tableNameClean, 
   			                   "true",tmpTableName,"hash"))*/
		return hiveContext.sql(qb.buildSelectQuery(selectionString2, 
   			                   tableNameClean, 
   			                   "true"))

	}

	/**This function takes a sample and a rdd of (Hash, Dup) and updates those records in the RDD.
	 * it returns a new updated SchemaRDD, and there is a persist flag to write these results to HIVE.
	 */
  private [sampleclean] def updateTableDuplicateCounts(tableName: String, rdd:RDD[(String, Double)], persist:Boolean = true): SchemaRDD= {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = qb.getCleanSampleName(tableName)
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		//hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceDupSchema(rdd)),"tmp")
		//hql("DROP TABLE tmp")
		hiveContext.createDataFrame(enforceDupSchema(rdd)).saveAsTable(tmpTableName)
		//hiveContext.sql(qb.createTableAs(tmpTableName) +qb.buildSelectQuery(List("*"),"tmp"))

		//Uses the hive API to get the schema of the table.
		var selectionString = List[String]()

		for (field <- getHiveTableSchema(tableNameClean))
		{
			if (field.equals("dup"))
				selectionString = typeSafeHQL(tmpTableName+".dup",1) :: selectionString // To Sanjay: It was tmpNameClean before. Since it failed to compile, I changed it to tableNameClean
			else
				selectionString = tableNameClean+"."+field :: selectionString
		}

		selectionString = selectionString.reverse

		//println(selectionString)

   		//applies hive query to update the data
   		if(persist){
   		    hiveContext.sql(qb.overwriteTable(tableNameClean) +
   		    				qb.buildSelectQuery(selectionString,
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))
   	   }

   		return hiveContext.sql(qb.buildSelectQuery(selectionString, 
   			                   tableNameClean, 
   			                   "true",tmpTableName,"hash"))

	}

	/**This function takes a sample and a rdd of (Hash) and keeps those records in the RDD.
	 * It returns a new updated SchemaRDD, and there is a persist flag to write these results to HIVE.
	 */
  private [sampleclean] def filterTable(tableName: String, rdd:RDD[String], persist:Boolean = true): SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = qb.getCleanSampleName(tableName)
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		
		//hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceFilterSchema(rdd)),"tmp")
		//hql("DROP TABLE tmp")
		hiveContext.createDataFrame(enforceFilterSchema(rdd)).saveAsTable(tmpTableName)

		//hiveContext.sql(qb.createTableAs(tmpTableName) + qb.buildSelectQuery(List("hash"),"tmp"))
		
		if(persist){
			hiveContext.sql(qb.overwriteTable(tableNameClean) +
   		    				qb.buildSelectSemiJoinQuery(List(tableNameClean+".*"),
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))
		}

		val result = hiveContext.sql(qb.buildSelectSemiJoinQuery(List(tableNameClean+".*"),
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))

		return result

	}

	/**
	 * Given a column name, a row, and a sampleName it returns the column as
	 * a string.
	 * @param sampleName a sample from which the row comes
	 * @param colName the name of the col you want to access
	 * @return in
	 */
  private [sampleclean] def getColAsIndex(sampleName:String, colName:String):Int=
    {
    	val tableNameClean = qb.getCleanSampleName(sampleName)
    	val schemaString = getHiveTableSchema(tableNameClean)
    	val index = schemaString.indexOf(colName.toLowerCase)
    	if(index >= 0)
    		return index
    	else
    		return -1
    }

  /**
   * Given a column name, a row, and a sampleName it returns the column as
   * a string.
   * @param sampleName a sample from which the row comes
   * @param colName the name of the col you want to access
   * @return in
   */
  private [sampleclean] def getColAsIndexFromBaseTable(sampleName:String, colName:String):Int=
    {
    	val tableNameClean = getParentTable(qb.getCleanSampleName(sampleName))
    	val schemaString = getHiveTableSchema(tableNameClean)
    	val index = schemaString.indexOf(colName.toLowerCase)
    	if(index >= 0)
    		return index
    	else
    		return -1
    }

  /**
   * Given a column name, a row, and a sampleName it returns the column as
   * a string.
   * @param row A row to query
   * @param sampleName a sample from which the row comes
   * @param colName the name of the col you want to access
   * @return string
   */
  private [sampleclean] def getColAsString(row:Row, sampleName:String, colName:String):String=
    {
    	val tableNameClean = qb.getCleanSampleName(sampleName)
    	val schemaString = getHiveTableSchema(tableNameClean)
    	val index = schemaString.indexOf(colName.toLowerCase)
    	if(index >= 0)
    		return row(index).asInstanceOf[String]
    	else
    		return null
    }

  /**
   * Given a column name, a row, and a baseTable it returns the column as
   * a string.
   * @param row A row to query
   * @param sampleName a sample from which the row comes
   * @param colName the name of the col you want to access
   * @return string
   */
  private [sampleclean] def getColAsStringFromBaseTable(row:Row, sampleName:String, colName:String):String=
    {
    	val tableNameClean = getParentTable(qb.getCleanSampleName(sampleName))
    	val schemaString = getHiveTableSchema(tableNameClean)
    	val index = schemaString.indexOf(colName.toLowerCase)
    	if(index >= 0)
    		return row.getString(index)
    	else
    		return null
    }

  /**
   * Given a column name, a row, and a sampleName it returns the column as
   * a double.
   * @param row A row to query
   * @param sampleName a sample from which the row comes
   * @param colName the name of the col you want to access
   * @return double
   */
  private [sampleclean] def getColAsDouble(row:Row, sampleName:String, colName:String):Double=
    {
    	val tableNameClean = qb.getCleanSampleName(sampleName)
    	val schemaString = getHiveTableSchema(tableNameClean)
    	val index = schemaString.indexOf(colName.toLowerCase)
    	if(index >= 0)
    		return row.getDouble(index)
    	else
    		return Double.NaN
    }

	/**Given a table name, this retrieves the schema as a list
	* from the Hive Catalog
	*/
  private [sampleclean] def getHiveTableSchema(tableName:String):List[String] = {
		var schemaList = List[String]()
		blocking { 
		
		try{
			val msc:HiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
			val sd:StorageDescriptor = msc.getTable(tableName).getSd();
			val fieldSchema = sd.getCols();
			for (field <- fieldSchema)
				schemaList = field.getName() :: schemaList 
		}
		catch {
     		case e: Exception => 0
   		}

   		}

		return schemaList.reverse
	}

  /**
   * Given a table name, this retrieves the schema as a list
   * from the Hive Catalog
   */
  private [sampleclean] def getTableContext(tableName:String):List[String] = {
		val cleanSampleName = qb.getCleanSampleName(tableName)
		var schemaList = List[String]()
		blocking { 
		
		try{
			val msc:HiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
			val sd:StorageDescriptor = msc.getTable(cleanSampleName).getSd();
			val fieldSchema = sd.getCols();
			for (field <- fieldSchema)
				schemaList = field.getName() :: schemaList 
		}
		catch {
     		case e: Exception => 0
   		}

   		}

		return schemaList.reverse
	}

  /**Given a table name and col names, this retrieves the indices of these cols in the table
    * from the Hive Catalog
    */
  private [sampleclean] def getColIndicesByNames(tableName:String, colNames:List[String]):List[Int] = {
    val schemaList = getHiveTableSchema(tableName)
    colNames.foldRight(List[Int]())((a, b) => schemaList.indexOf(a) :: b)
  }

  /**Given a table name and a col name, this retrieves the index of the col in the table
    * from the Hive Catalog
    */
  private [sampleclean] def getColIndexByName(tableName:String, colName:String):Int = {
    val schemaList = getHiveTableSchema(tableName)
    schemaList.indexOf(colName)
  }

  /**Return a map that is from col names to col indices for the clean sample table
    */
  private [sampleclean] def getSampleTableColMapper(sampleName:String):List[String] => List[Int] = {
    val cleanSampleName = qb.getCleanSampleName(sampleName)
    getColIndicesByNames(cleanSampleName, _: List[String])
  }

  /**Return a map that is from col names to col indices for the full table
    */
  private [sampleclean] def getFullTableColMapper(sampleName:String):List[String] => List[Int] = {
    val fullTableName = getParentTable(qb.getCleanSampleName(sampleName))
    getColIndicesByNames(fullTableName, _: List[String])
  }

  /** This function returns all the tables we have created in this session
	as temporary tables.
	*/
  private [sampleclean] def getAllTempTables():List[String] = {
		var dbList = List[String]()
		var tableList = List[String]()
		try{
			val msc:HiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
			for(l <- msc.getAllDatabases())
				dbList = l :: dbList

			var tmpTableList = List[String]()
   			for(d <- dbList)
   			{
   				for(t <- msc.getAllTables(d))
   				{
   					if ((t contains "tmp") || 
   						(t contains "_base") ||
   						(t contains "_clean") || 
   						(t contains "_dirty") )
   					{
   						tableList = t :: tableList
   					}
   				}
   			}
		}
		catch {
     		case e: Exception => 0
   		}


		return tableList
	}

	/*This function uses the hive catalog to get the parent table
	 */
  private [sampleclean] def getParentTable(tableName:String):String =
	{
		try{
			val msc:HiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
			return msc.getTable(tableName).getParameters().get("comment").trim.split(" ")(0)
		}
		catch {
     		case e: Exception => 0
   		}
   		return ""
	}

  /**
   * Returns the sampling ratio used to create a sample table.
   * The table should exist in Hive. The clean sample name can be
   * accessed through getCleanSampleName from the class val qb
   * @param tableName table name
   */
	def getSamplingRatio(tableName:String):Double =
	{
		try{
			val msc:HiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
			return msc.getTable(tableName).getParameters().get("comment").trim.split(" ")(1).toDouble
		}
		catch {
     		case e: Exception => println(e)
   		}
   		return 1.0
	}
}

//This object provides some helper methods from the SampleCleanContext
@serializable
private [sampleclean] object SampleCleanContext {
	//two case clases that can help force typing
	case class FilterTuple(hash: String)
	case class DupTuple(hash: String, dup: Int)
	case class SSTuple(hash: String, updateAttr:String)
	case class SDTuple(hash: String, updateAttr:Double)

	//These methods take un-named cols in RDDs and force them
	//into named cols.
	def enforceFilterSchema(rdd:SchemaRDD): RDD[FilterTuple] = {
		return rdd.map( x => FilterTuple(x(0).asInstanceOf[String]))
	}

	def enforceFilterSchema(rdd:RDD[String]): RDD[FilterTuple] = {
		return rdd.map( x => FilterTuple(x.asInstanceOf[String]))
	}

	def enforceDupSchema(rdd:RDD[(String, Double)]): RDD[DupTuple] = {
		return rdd.map( x => DupTuple(x._1.asInstanceOf[String],x._2.asInstanceOf[Int]))
	}

	def enforceDupSchema(rdd:SchemaRDD): RDD[DupTuple] = {
		return rdd.map( x => DupTuple(x(0).asInstanceOf[String],x(1).asInstanceOf[Int]))
	}

	def enforceSSSchema(rdd:RDD[(String, String)]): RDD[SSTuple] = {
		return rdd.map( x => SSTuple(x._1.asInstanceOf[String],x._2.asInstanceOf[String]))
	}

	def enforceSSSchema(rdd:SchemaRDD): RDD[SSTuple] = {
		return rdd.map( x => SSTuple(x(0).asInstanceOf[String],x(1).asInstanceOf[String]))
	}

	def enforceSDSchema(rdd:RDD[(String, Double)]): RDD[SDTuple] = {
		return rdd.map( x => SDTuple(x._1.asInstanceOf[String],x._2.asInstanceOf[Double]))
	}

	def enforceSDSchema(rdd:SchemaRDD): RDD[SDTuple] = {
		return rdd.map( x => SDTuple(x(0).asInstanceOf[String],x(1).asInstanceOf[Int]))
	}

}
