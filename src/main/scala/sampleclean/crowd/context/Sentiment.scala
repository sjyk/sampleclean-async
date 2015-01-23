package sampleclean.crowd.context

/**
 * Parameters for labeling a single tweet
 * @param content the text of the tweet.
 */
case class SentimentPointLabelingContext(content: String) extends PointLabelingContext


/**  Group Context subclass for sentiment analysis, the data should be an empty map
  * @param taskType the type of all the points in the group
  * @param data the group context that is shared among all the points
  */
case class SentimentGroupLabelingContext(taskType: String="sa", data: Map[String, String]) extends GroupLabelingContext