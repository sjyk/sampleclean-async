package sampleclean.clean.featurize
import org.apache.spark.sql.{SchemaRDD, Row}
import uk.ac.shef.wit.simmetrics.similaritymetrics._

/* This class implements the similarity based featurizer used in Deduplication
 */
@serializable
abstract class BlockingFeaturizer(cols: List[Int], 
								  val tokenizer:Tokenizer, 
								  val threshold:Double,
                  val minSize: Int = 0)
	extends Featurizer(cols){

		val canPrefixFilter: Boolean

		//val tokenizer = tokenizer

		def featurize[K,V](rows: Set[Row], params: collection.immutable.Map[K,V]=null): (Set[Row], Array[Double]) = {

			val rowA = rows.head
			val rowB = rows.last

			var stringA = ""
			var stringB = ""
			for (col <- cols){
				stringA = stringA + " " + rowA(col).asInstanceOf[String]
				stringB = stringB + " " + rowB(col).asInstanceOf[String]
			}

			val tokens1 = tokenizer.tokenSet(stringA)
			val tokens2 = tokenizer.tokenSet(stringB)
			var tokenWeights = Map[String,Double]()

			if(params != null)
				tokenWeights = params.asInstanceOf[Map[String,Double]]

			val simVal = similar(tokens1, tokens2, threshold, tokenWeights)

			var sim = 0.0
			if (simVal)
				sim = 1.0

			return (Set(rowA, rowB),
					Array(sim))
		}

		def similar(tokens1:Seq[String],
					  tokens2: Seq[String], 
					  thresh:Double,
					  tokenWeights: collection.Map[String, Double]): Boolean

		 /**
   		  * Computes the number of tokens that can be removed from the tokenSet as per Prefix Filtering algorithm.
   		  * @param sortedTokens  token list. Must be sorted as per tokens' corresponding weights.
   		  * @param modThreshold modified threshold that depends on selected similarity measure.
   		  */
  		def getRemovedSize (sortedTokens: Seq[String], modThreshold: Double, tokenWeights: collection.Map[String, Double]): Int = {
    		if (canPrefixFilter) {
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
  		def sumWeight (tokens: Seq[String], tokenWeights: collection.Map[String, Double]): Double = {
      		tokens.foldLeft(0.0) ((accum, token) => accum + tokenWeights.getOrElse(token, 1.0))
  		}
}


/**
 * This class represents a similarity join based on the Jaccard similarity measure.
 * Token global weights are taken into account.
 */
class WeightedJaccardBlocking(cols: List[Int], 
							  tokenizer:Tokenizer, 
							  threshold:Double) 
	extends BlockingFeaturizer(cols, tokenizer, threshold) {

  val canPrefixFilter = true

  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  def similar (tokens1: Seq[String],
                 tokens2: Seq[String],
                 threshold: Double,
                 tokenWeights: collection.Map[String, Double]): Boolean = {

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
      false
    else
      intersectionWeight.toDouble / unionWeight + 1e-6 >= threshold
  }

  def getSimilarity(tokens1: Seq[String], tokens2: Seq[String],
                    tokenWeights: collection.Map[String, Double]): Double = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)
    val unionWeight = weight1 + weight2 - intersectionWeight

    if (unionWeight == 0)
      0
    else
      intersectionWeight.toDouble / unionWeight + 1e-6

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
 * This class represents a similarity join based on the overlap between two lists.
 * Token global weights are taken into account.
 */
class WeightedOverlapBlocking(cols: List[Int], 
							  tokenizer:Tokenizer, 
							  threshold:Double) 
	extends BlockingFeaturizer(cols, tokenizer, threshold) {

  val canPrefixFilter = true
  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map         
   * @return
   */
  def similar(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): Boolean = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    if (weight1 < weight2)
      if (weight1 < threshold) false
    else
      if (weight2 < threshold) false

    sumWeight(tokens1.intersect(tokens2), tokenWeights) >= threshold
  }

  def getSimilarity(tokens1: Seq[String], tokens2: Seq[String],
                    tokenWeights: collection.Map[String, Double]): Double = {

    sumWeight(tokens1.intersect(tokens2), tokenWeights)

  }


  /**
   * Calls getRemovedSize method with overlap-based parameters
   * @param tokens token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map
   */
  @Override
  override def getRemovedSize(tokens: Seq[String], threshold: Double, tokenWeights: collection.Map[String, Double]): Int ={
    super.getRemovedSize(tokens, threshold, tokenWeights)
  }

}

/**
 * This class represents a similarity join based on the Dice similarity measure.
 * Token global weights are taken into account.
 */
class WeightedDiceBlocking(cols: List[Int], 
							  tokenizer:Tokenizer, 
							  threshold:Double)
	extends BlockingFeaturizer(cols, tokenizer, threshold) {

   val canPrefixFilter = true

  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map         
   */
  def similar(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): Boolean = {

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
      false
    else
      2 * intersectionWeight.toDouble / weightSum >= threshold

  }

  def getSimilarity(tokens1: Seq[String], tokens2: Seq[String],
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
 * Token global weights are taken into account.
 */
class WeightedCosineBlocking(cols: List[Int], 
							  tokenizer:Tokenizer, 
							  threshold:Double)
	extends BlockingFeaturizer(cols, tokenizer, threshold) {

   val canPrefixFilter = true
  /**
   * Returns true if two token lists are similar; otherwise, returns false
   * @param tokens1 first token list.
   * @param tokens2 second token list.
   * @param threshold specified threshold.
   * @param tokenWeights token-to-weight map         
   */
  def similar(tokens1: Seq[String],
                tokens2: Seq[String],
                threshold: Double,
                tokenWeights: collection.Map[String, Double]): Boolean = {

    val weight1 = sumWeight(tokens1, tokenWeights)
    val weight2 = sumWeight(tokens2, tokenWeights)

    //Length Filtering
    val weightSqrt = math.sqrt(weight1 * weight2)
    if (weight1 < weight2)
      if (weight1 < weightSqrt*threshold) false
    else
      if (weight2 < weightSqrt*threshold) false

    val intersectionWeight = sumWeight(tokens1.intersect(tokens2), tokenWeights)

    if (weightSqrt == 0)
      false
    else
      intersectionWeight / weightSqrt >= threshold

  }

  def getSimilarity(tokens1: Seq[String], tokens2: Seq[String],
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