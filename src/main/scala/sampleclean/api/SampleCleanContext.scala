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
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import SampleCleanContext._
import sampleclean.util.TypeUtils._
import sampleclean.util.QueryBuilder

/** As an analog to the SparkContext, the SampleCleanContext
*gives a handle to the current session. This class provides
*the basic API to manipulate the data structures. We assume
*that the data is initially in a HIVE store.
*  
*In its current implementation, the SampleCleanContext
*supports both persistent data in HIVE or keeping the data
*in memory as an RDD. 
*/
@serializable
class SampleCleanContext(@transient sc: SparkContext) {

	//Use these functions to access the Spark
	//and Hive contexts in API calls.

	/**Returns the HiveContext
	*/
	def getHiveContext():HiveContext = {
		return new HiveContext(sc)
	}

	/**Returns the SparkContext
	*/
	def getSparkContext():SparkContext = {
		return sc
	}

	//a refernce to the contexts Query Builder
	val qb = new QueryBuilder(this)

    /** This function initializes the clean and dirty samples as
    * Schema RDD's in a tuple (Clean, Dirty). There is an additional
    * flag to persist the rdd in HIVE if desired.
    *
    * Args base table, sample name, sampling ratio, persist (optional)
    */
	def initialize(baseTable:String, tableName:String, samplingRatio: Double, persist:Boolean=true): (SchemaRDD, SchemaRDD) = {

		val hiveContext = new HiveContext(sc)
		//creates the clean sample using table sampling procedure
		//when databricks gives us a better implementation of sampling
		//we can use that. 
		val selectionList = List("reflect(\"java.util.UUID\", \"randomUUID\") as hash",
			                     "1 as dup", "*")

		if(persist)
		{
			var query = qb.createTableAs(qb.getCleanSampleName(tableName)) +
			qb.buildSelectQuery(selectionList,baseTable) +
			qb.tableSample(samplingRatio)

			hiveContext.hql(query)

			hiveContext.hql(qb.setTableParent(qb.getCleanSampleName(tableName),baseTable + " " + samplingRatio))

			query = qb.createTableAs(qb.getDirtySampleName(tableName)) +
					qb.buildSelectQuery(List("*"),qb.getCleanSampleName(tableName))
		

			hiveContext.hql(query)
		}
		
		return (hiveContext.hql(qb.buildSelectQuery(selectionList,baseTable) +
					qb.tableSample(samplingRatio)), 
				hiveContext.hql(qb.buildSelectQuery(List("*"),
					qb.getCleanSampleName(tableName))))
	}

	/* This function cleans up after using initializeHive by dropping
	 * any temp tables. If you use hive and don't execute this command, 
	 * the samples can persist between sessions as it will be written
	 * to disk.
	 */
	def closeHiveSession() = {
		val hiveContext = new HiveContext(sc)

		for (t <- getAllTempTables())
		{
			hiveContext.hql("DROP TABLE IF EXISTS " + t)
		}
	}

	/* Returns an RDD which points to a full table
	*/
	def getFullTable(sampleName: String):SchemaRDD = {

		val hiveContext = new HiveContext(sc)

		return hiveContext.hql(qb.buildSelectQuery(List("*"),
							  getParentTable(qb.getCleanSampleName(sampleName))))
	}

	/* Returns an RDD which points to a sample 
	 * of a full table.
	 */
	def getCleanSample(tableName: String):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		return hiveContext.hql(qb.buildSelectQuery(List("*"),qb.getCleanSampleName(tableName)))
	}

	/* Returns an RDD which points to a sample 
	 * of a full table. This method returns a tuple
	 * of the hash and the requested attribute
	 */
	def getCleanSampleAttr(tableName: String, attr: String):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		return hiveContext.hql(qb.buildSelectQuery(List("hash",attr),qb.getCleanSampleName(tableName)))
	}

	/* Returns an RDD which points to a sample 
	 * of a full table. This method returns a tuple
	 * of the hash and the requested attribute
	 */
	def getCleanSampleAttr(tableName: String, attr: String, pred:String):SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		return hiveContext.hql(qb.buildSelectQuery(List("hash",attr),qb.getCleanSampleName(tableName), pred))
	}

		/**This function takes a sample and a rdd of (Hash, Val) and updates those records in the RDD.
	 * it returns a new updated SchemaRDD, and there is a persist flag to write these results to HIVE.
	 */
	def updateTableAttrValue(tableName: String, attr:String, rdd:RDD[(String, String)], persist:Boolean = true): SchemaRDD= {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = qb.getCleanSampleName(tableName)
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceSSSchema(rdd)),"tmp")
		hiveContext.hql(qb.createTableAs(tmpTableName) +qb.buildSelectQuery(List("*"),"tmp"))

		//Uses the hive API to get the schema of the table.
		var selectionString = List[String]()

		for (field <- getHiveTableSchema(tableNameClean))
		{
			if (field.equals(attr))
				selectionString = qb.makeExpressionExplicit("updateAttr",tmpTableName) :: selectionString // To Sanjay: It was tmpNameClean before. Since it failed to compile, I changed it to tableNameClean
			else
				selectionString = qb.makeExpressionExplicit(field,tableNameClean) :: selectionString
		}

		selectionString = selectionString.reverse

   		//applies hive query to update the data
   		if(persist){
   		    hiveContext.hql(qb.overwriteTable(tableNameClean) +
   		    				qb.buildSelectQuery(selectionString,
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))
   	   }

   		return hiveContext.hql(qb.buildSelectQuery(selectionString, 
   			                   tableNameClean, 
   			                   "true",tmpTableName,"hash"))

	}

	/**This function takes a sample and a rdd of (Hash, Dup) and updates those records in the RDD.
	 * it returns a new updated SchemaRDD, and there is a persist flag to write these results to HIVE.
	 */
	def updateTableDuplicateCounts(tableName: String, rdd:RDD[(String, Int)], persist:Boolean = true): SchemaRDD= {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = qb.getCleanSampleName(tableName)
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceDupSchema(rdd)),"tmp")
		hiveContext.hql(qb.createTableAs(tmpTableName) +qb.buildSelectQuery(List("*"),"tmp"))

		//Uses the hive API to get the schema of the table.
		var selectionString = List[String]()

		for (field <- getHiveTableSchema(tableNameClean))
		{
			if (field.equals("dup"))
				selectionString = typeSafeHQL(qb.makeExpressionExplicit("dup",tmpTableName),1) :: selectionString // To Sanjay: It was tmpNameClean before. Since it failed to compile, I changed it to tableNameClean
			else
				selectionString = qb.makeExpressionExplicit(field,tableNameClean) :: selectionString
		}

		selectionString = selectionString.reverse

   		//applies hive query to update the data
   		if(persist){
   		    hiveContext.hql(qb.overwriteTable(tableNameClean) +
   		    				qb.buildSelectQuery(selectionString,
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))
   	   }

   		return hiveContext.hql(qb.buildSelectQuery(selectionString, 
   			                   tableNameClean, 
   			                   "true",tmpTableName,"hash"))

	}

	/**This function takes a sample and a rdd of (Hash) and keeps those records in the RDD.
	 * It returns a new updated SchemaRDD, and there is a persist flag to write these results to HIVE.
	 */
	def filterTable(tableName: String, rdd:RDD[String], persist:Boolean = true): SchemaRDD = {
		val hiveContext = new HiveContext(sc)
		val sqlContext = new SQLContext(sc)
		val tableNameClean = qb.getCleanSampleName(tableName)
		val tmpTableName = "tmp"+Math.abs((new Random().nextLong()))
		hiveContext.registerRDDAsTable(sqlContext.createSchemaRDD(enforceFilterSchema(rdd)),"tmp")

		hiveContext.hql(qb.createTableAs(tmpTableName) + qb.buildSelectQuery(List("hash"),"tmp"))
		
		if(persist){
			hiveContext.hql(qb.overwriteTable(tableNameClean) +
   		    				qb.buildSelectSemiJoinQuery(List(tableNameClean+".*"),
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))
		}

		val result = hiveContext.hql(qb.buildSelectSemiJoinQuery(List(tableNameClean+".*"),
   		    					             tableNameClean,
   		    					             "true",tmpTableName,
   		    					             "hash"))

		return result

	}

	/**
	 * Given a column name, a row, and a sampleName it returns the column as
	 * a string.
	 * @param row A row to query
	 * @param sampleName a sample from which the row comes
	 * @param colName the name of the col you want to access
	 * @return string
	 */
	def getColAsString(row:Row, sampleName:String, colName:String):String=
    {
    	val tableNameClean = qb.getCleanSampleName(sampleName)
    	val schemaString = getHiveTableSchema(tableNameClean)
    	val index = schemaString.indexOf(colName.toLowerCase)
    	if(index >= 0)
    		return row.getString(index)
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
    def getColAsStringFromBaseTable(row:Row, sampleName:String, colName:String):String=
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
    def getColAsDouble(row:Row, sampleName:String, colName:String):Double=
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
	def getHiveTableSchema(tableName:String):List[String] = {
		var schemaList = List[String]()
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

		return schemaList.reverse
	}

	/** This function returns all the tables we have created in this session
	as temporary tables.
	*/
	def getAllTempTables():List[String] = {
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
	def getParentTable(tableName:String):String =
	{
		try{
			val msc:HiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
			return msc.getTable(tableName).getParameters().get("comment").split(" ")(0)
		}
		catch {
     		case e: Exception => 0
   		}
   		return ""
	}

	/*This function uses the hive catalog to get the parent table
	 */
	def getSamplingRatio(tableName:String):Double =
	{
		try{
			val msc:HiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
			return msc.getTable(tableName).getParameters().get("comment").split(" ")(1).toDouble
		}
		catch {
     		case e: Exception => 0
   		}
   		return 1.0
	}
}

//This object provides some helper methods from the SampleCleanContext
@serializable
object SampleCleanContext {
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

	def enforceDupSchema(rdd:RDD[(String, Int)]): RDD[DupTuple] = {
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
