package sampleclean.clean.featurize

import org.apache.spark.sql.Row
import scala.collection.Seq

/** 
 * One particular use for similarity featurizers is in Similarity joins.
 * A Similarity Join compares pairs of rows and decides whether they are similar
 * or not. To compare a certain pair of rows, a Similarity Featurizer would
 * use some criteria to output a single pair feature:
 * 1.0 if they are similar or 0.0 otherwise.
 *
 * A special class of Similarity Featurizers have properties
 * that allow for a type of optimization called Prefix Filtering.
 *
 * We encode this logic into AnnotatedSimilarityFeaturizer.
 *
 * @param colNames the names of the columns that will be used for pairwise comparisons
 * @param context names of all columns for the existing dataset
 * @param tokenizer Function used to tokenize Strings
 * @param threshold depends on similarity function used
 * @param minSize this parameter is used by Prefix Filtering to filter out
 *                pairs of records that are similar but are too short
 *                to be considered "strongly" similar. For example, [Bob, bob]
 *                could be considered similar by the algorithm but not by a person
 *                since there is lack information about Bob's last name. In this case,
 *                minSize could be set to 2: pair members with token sizes
 *                less than 2 will be omitted.
 * @param schemaMap maps columns from one table to another. Will assume equal schema if null.
 */
@serializable
abstract class AnnotatedSimilarityFeaturizer(val colNames: List[String], 
                  context:List[String],
								  val tokenizer:Tokenizer, 
								  var threshold:Double,
                  val minSize: Int,
                  val schemaMap: Map[Int,Int]= null)
	extends Featurizer(colNames, context){

		val usesTokenPrefixFiltering: Boolean
    val usesStringPrefixFiltering: Boolean

  /**
   * This function takes a set of rows, takes a token-to-weight map and
   * calculates whether the set is similar or not.
   *
   * @param rows rows used for comparison.
   *             If the set is larger than 2, the algorithm
   *             compares the first and last row in the set.
   * @param params token-to-weight map
   * @tparam K String (tokens)
   * @tparam V Double (weights)
   * @return The first element of the tuple is a set of primary keys and the second
   *         element is Array(1.0) if the pair is similar, Array(0.0) otherwise.
   */
		def featurize[K,V](rows: Set[Row], params: collection.immutable.Map[K,V]=null): (Set[Row], Array[Double]) = {

			val rowA = rows.head
			val rowB = rows.last

			var stringA = ""
			var stringB = ""
			for (col <- cols){
				stringA = stringA + " " + rowA(col).asInstanceOf[String]

        if(schemaMap == null)
				  stringB = stringB + " " + rowB(col).asInstanceOf[String]
        else if(schemaMap.contains(col))
          stringB = stringB + " " + rowB(schemaMap(col)).asInstanceOf[String]
        else
          throw new RuntimeException("The schemas do not align up between your tables")
			}

			val tokens1 = tokenizer.tokenSet(stringA)
			val tokens2 = tokenizer.tokenSet(stringB)
			var tokenWeights = Map[String,Double]()

			if(params != null)
				tokenWeights = params.asInstanceOf[Map[String,Double]]

			val simVal = optimizedSimilarity(tokens1, tokens2, threshold, tokenWeights)._1

			var sim = 0.0
			if (simVal)
				sim = 1.0

			return (Set(rowA, rowB),
					Array(sim))
		}

  /**
   * This function takes a set of rows, takes a token-to-weight map and
   * calculates whether the set is similar or not.
   *
   * @param rows rows used for comparison.
   *             If the set is larger than 2, the algorithm
   *             compares the first and last row in the set.
   * @param params token-to-weight map
   * @tparam K String (tokens)
   * @tparam V Double (weights)
   * @return The first element of the tuple is a set of primary keys and the second
   *         element is Array(Similarity Value)
   */
    def getSimilarityDouble[K,V](rows: Set[Row], params: collection.immutable.Map[K,V]=null): (Set[Row], Double) = {

      val rowA = rows.head
      val rowB = rows.last

      var stringA = ""
      var stringB = ""
      for (col <- cols){
        stringA = stringA + " " + rowA(col).asInstanceOf[String]

        if(schemaMap == null)
          stringB = stringB + " " + rowB(col).asInstanceOf[String]
        else if(schemaMap.contains(col))
          stringB = stringB + " " + rowB(schemaMap(col)).asInstanceOf[String]
        else
          throw new RuntimeException("The schemas do not align up between your tables")
      }

      val tokens1 = tokenizer.tokenSet(stringA)
      val tokens2 = tokenizer.tokenSet(stringB)
      var tokenWeights = Map[String,Double]()

      if(params != null)
        tokenWeights = params.asInstanceOf[Map[String,Double]]

      val simVal = optimizedSimilarity(tokens1, tokens2, threshold, tokenWeights)._2

      return (Set(rowA, rowB),
          simVal)
    }

    //TODO Fix A,B
    private [sampleclean] def getCols(a:Boolean = true):List[Int] ={
        if(a || schemaMap == null)
        {
          return cols
        }
        else
        {
          var result:List[Int] = List()
          for (col <- cols){
        
          if(!schemaMap.contains(col))
            throw new RuntimeException("The schemas do not align up between your tables")

          result = schemaMap(col) :: result

          }
          return result.reverse
        }
    }

  /**
   * Calculates similarity between two lists of tokens.
   *
   * Uses a known threshold > 0 for optimization. If no threshold > 0
   * is specified, use [[similarity()]]
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param thresh specified threshold.
   * @param tokenWeights token-to-weight map
   * @return (true if they are similar, similarity)
   */
		def optimizedSimilarity(tokens1:Seq[String],
					  tokens2: Seq[String], 
					  thresh:Double,
					  tokenWeights: collection.Map[String, Double]): (Boolean,Double)


  /**
   * Calculates similarity between two lists of tokens.
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param tokenWeights token-to-weight map
   */
    def similarity(tokens1: Seq[String], tokens2: Seq[String],
                    tokenWeights: collection.Map[String, Double]): Double

		 /**
   		  * Computes the number of tokens that can be removed from the tokenSet as per Prefix Filtering algorithm.
   		  * @param sortedTokens  token list. Must be sorted as per tokens' corresponding weights.
   		  * @param modThreshold modified threshold that depends on selected similarity measure.
   		  */
     private [sampleclean] def getRemovedSize (sortedTokens: Seq[String], modThreshold: Double, tokenWeights: collection.Map[String, Double]): Int = {
    		if (usesTokenPrefixFiltering) {
            val weighted = tokenWeights.size != 0
            val removedSize = {
              sortedTokens.foldRight((0.0, 0)) {
              case (token, (accum, count)) => {
                  // weight is 0 if token does not have an assigned weight
                  val current = accum + (if (weighted) tokenWeights.getOrElse(token, 0.0) else 1.0)

                  if (current < modThreshold) (current, count + 1) else (current, count)
                }
                }._2
            }

            if (removedSize > sortedTokens.size)
                return sortedTokens.size
            else if (removedSize < 0)
                return 0
            else
                return removedSize
      		}
      		else
      			return 0
  		}

  		/**
   		* Computes the sum of individual token weights over a token list.
   		* If a token is not found on the given map, it assumes the token has a weight of 0.
   		* @param tokens token list to be weighted
   		* @param tokenWeights token-to-weight map
   		*/
  		private [sampleclean] def sumWeight (tokens: Seq[String], tokenWeights: collection.Map[String, Double]): Double = {
        if (tokenWeights.isEmpty) tokens.size
        else tokens.foldLeft(0.0)((accum, token) => accum + tokenWeights.getOrElse(token, 0.0))

  		}

      override def toString():String = {
        return this.getClass.getSimpleName + " @ " + threshold
      }

}
object AnnotatedSimilarityFeaturizer{
/**
 * This class represents a similarity join based on the Jaccard similarity measure.
 */
 class WeightedJaccardSimilarity(colNames: List[String], 
                  context:List[String], 
							    tokenizer:Tokenizer,
							    threshold:Double = 0,
                  minSize: Int = 0)
	extends AnnotatedSimilarityFeaturizer(colNames, context, tokenizer, threshold, minSize) {

  val usesTokenPrefixFiltering = true
  val usesStringPrefixFiltering = false

  def optimizedSimilarity(tokens1: Seq[String],
                 tokens2: Seq[String],
                 threshold: Double,
                 tokenWeights: collection.Map[String, Double]): (Boolean,Double) = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    if (weight1 < weight2)
      if (weight1 < weight2*threshold) false
    else
      if (weight2 < weight1*threshold) false

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)
    val unionWeight = weight1 + weight2 - intersectionWeight

    if (unionWeight == 0)
      return (false,0.0)
    else
      return (intersectionWeight.toDouble / unionWeight + 1e-6 >= threshold, intersectionWeight.toDouble / unionWeight)
  }

  def similarity(tokens1: Seq[String], tokens2: Seq[String],
                    tokenWeights: collection.Map[String, Double]): Double = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)
    val unionWeight = weight1 + weight2 - intersectionWeight

    if (unionWeight == 0)
      0
    else
      intersectionWeight.toDouble / unionWeight

  }


  /**
   * Calls getRemovedSize method with Jaccard-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  @Override
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
    val weight = {
      if (tokenWeights.size == 0) tokens.length
      else sumWeight(tokens, tokenWeights)
    }
    super.getRemovedSize(tokens, threshold * weight, tokenWeights)
  }

}

/**
 * This class represents a similarity join based on the Overlap similarity measure.
 */
class WeightedOverlapSimilarity(colNames: List[String], 
                  context:List[String], 
							  tokenizer:Tokenizer,
							  threshold:Double = 0,
                minSize: Int = 0)
	extends AnnotatedSimilarityFeaturizer(colNames, context, tokenizer, threshold, minSize) {

  val usesTokenPrefixFiltering = true
  val usesStringPrefixFiltering = false

  def optimizedSimilarity(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): (Boolean, Double) = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    if (weight1 < weight2)
      if (weight1 < threshold) false
    else
      if (weight2 < threshold) false

      return(sumWeight(tokens1.intersect(tokens2), tokenWeights) >= threshold, sumWeight(tokens1.intersect(tokens2), tokenWeights) )
  }

  def similarity(tokens1: Seq[String], tokens2: Seq[String],
                    tokenWeights: collection.Map[String, Double]): Double = {

    sumWeight(tokens1.intersect(tokens2), tokenWeights)

  }


  /**
   * Calls getRemovedSize method with Jaccard-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
    super.getRemovedSize(tokens, threshold, tokenWeights)
  }

}
/**
 * This class represents a similarity join based on the Dice similarity measure.
 *
 */
class WeightedDiceSimilarity(colNames: List[String], 
                  context:List[String], 
							    tokenizer:Tokenizer,
							    threshold:Double = 0,
                  minSize: Int = 0 )
	extends AnnotatedSimilarityFeaturizer(colNames, context, tokenizer, threshold, minSize) {

  val usesTokenPrefixFiltering = true
  val usesStringPrefixFiltering = false

  def optimizedSimilarity(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): (Boolean,Double) = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    val weightSum = weight1 + weight2
    if (weight1 < weight2)
      if (2*weight1 < weightSum*threshold) false
    else  
      if (2*weight2 < weightSum*threshold) false

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)

    if (weightSum == 0)
      return (false, 0.0)
    else
      return (2 * intersectionWeight.toDouble / weightSum >= threshold, 2 * intersectionWeight.toDouble / weightSum)

  }

  def similarity(tokens1: Seq[String], tokens2: Seq[String],
                    tokenWeights: collection.Map[String, Double]): Double = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    val weightSum = weight1 + weight2
    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)

    if (weightSum == 0)
      0
    else
      2 * intersectionWeight.toDouble / weightSum

  }

 /**
   * Calls getRemovedSize method with Dice-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  @Override
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
   val weight = {
     if (tokenWeights.size == 0) tokens.length
     else sumWeight(tokens, tokenWeights)
   }
    super.getRemovedSize(tokens, threshold * weight / (2 - threshold), tokenWeights)
  }


}

/**
 * This class represents a similarity join based on the Cosine similarity measure.
 */
class WeightedCosineSimilarity(colNames: List[String], 
                  context:List[String], 
							    tokenizer:Tokenizer,
							    threshold:Double = 0,
                  minSize: Int = 0)
	extends AnnotatedSimilarityFeaturizer(colNames, context, tokenizer, threshold, minSize) {

  val usesTokenPrefixFiltering = true
  val usesStringPrefixFiltering = false

  def optimizedSimilarity(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): (Boolean,Double) = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    val weightSqrt = math.sqrt(weight1 * weight2)
    if (weight1 < weight2)
      if (weight1 < weightSqrt*threshold) return(false,0.0)
    else
      if (weight2 < weightSqrt*threshold) return(false,0.0)

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)

    if (weightSqrt == 0)
      return (false,0.0)
    else
      return (intersectionWeight / weightSqrt >= threshold, intersectionWeight / weightSqrt)

  }

  def similarity(tokens1: Seq[String], tokens2: Seq[String],
                    tokenWeights: collection.Map[String, Double]): Double = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    val weightSqrt = math.sqrt(weight1 * weight2)
    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)

    if (weightSqrt == 0)
      0
    else
      intersectionWeight / weightSqrt

  }

  /**
   * Calls getRemovedSize method with Cosine-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  @Override
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
    val weight = {
      if (tokenWeights.size == 0) tokens.length
      else sumWeight(tokens, tokenWeights)
    }
    super.getRemovedSize(tokens, weight * math.pow(threshold, 2), tokenWeights)
  }

}
/**
 * This class represents a similarity join based on the Levenshtein (aka Edit Distance)
 * similarity measure. Token weights cannot be taken into
 * account since this is fundamentally a string-string
 * comparison measure.
 *
 * This measure is optimized for the Pass Join.
 */
  class EditFeaturizer(colNames: List[String],
                            context:List[String],
                               tokenizer:Tokenizer,
                               threshold:Double = 0)
    extends  AnnotatedSimilarityFeaturizer(colNames, context, tokenizer, threshold, 0) {

    val usesTokenPrefixFiltering = false
    val usesStringPrefixFiltering = true


    def optimizedSimilarity(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double] = null): (Boolean,Double) = {

      if (threshold == 0) {
        val sim = similarity(tokens1,tokens2,null)
        (sim <= threshold, sim)
      }
      else {
        val thresholdInt = threshold.toInt
        val _s = tokens1.mkString(" ")
        val _t = tokens2.mkString(" ")
        val (s, t) = if (_s.length > _t.length) (_s, _t) else (_t, _s)
        val n = s.length
        val m = t.length

        if (n - m > thresholdInt) return (false, thresholdInt + 1)

        var V = new Array[Array[Int]](thresholdInt * 3 + 2)
        for (i <- 0 until V.length)
          V(i) = Array.fill[Int](2)(Int.MinValue)

        V(-1 + thresholdInt + 1)(-1 & 1) = 0

        for (p <- 0 until thresholdInt + 1) {
          val f = p & 1
          val g = f ^ 1

          for (k <- thresholdInt + 1 - p until thresholdInt + 1 + p + 1) {
            V(k)(f) = math.max(math.max(V(k)(g), V(k + 1)(g)) + 1, V(k - 1)(g))
            val d = k - thresholdInt - 1
            if (V(k)(f) >= 0 && V(k)(f) + d >= 0)
              while (V(k)(f) < n && V(k)(f) + d < m && s(V(k)(f)) == t(V(k)(f) + d))
                V(k)(f) += 1
          }
          if (V(m - n + thresholdInt + 1)(f) >= n) return (p <= threshold, p)
        }
        (false, (thresholdInt + 1).toDouble)
      }


    }

    // based on Wikipedia article
    def similarity(tokens1: Seq[String], tokens2: Seq[String], tokenWeights: collection.Map[String, Double] = null): Double = {

      val str1 = tokens1.mkString(" ")
      val str2 = tokens2.mkString(" ")

      val lenStr1 = str1.length
      val lenStr2 = str2.length

      val d: Array[Array[Int]] = Array.ofDim(lenStr1 + 1, lenStr2 + 1)

      for (i <- 0 to lenStr1) d(i)(0) = i
      for (j <- 0 to lenStr2) d(0)(j) = j

      for (i <- 1 to lenStr1; j <- 1 to lenStr2) {
        val cost = if (str1(i - 1) == str2(j-1)) 0 else 1

        d(i)(j) = min(
          d(i-1)(j  ) + 1,     // deletion
          d(i  )(j-1) + 1,     // insertion
          d(i-1)(j-1) + cost   // substitution
        )
      }

      d(lenStr1)(lenStr2)
    }

  private def min(nums: Int*): Int = nums.min



  }




}