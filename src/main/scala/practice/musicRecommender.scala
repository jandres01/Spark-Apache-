package practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.recommendation.ALS

case class UserArtistData(userID: Int, artistID: Int, count: Int)

/**
 * Audioscrobbler data from: http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html
 * 
 */
object MusicRecommender extends App {
  val spark = SparkSession.builder.appName("Linear Regression").master("local[*]").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val userArtistData = spark.read.schema(Encoders.product[UserArtistData].schema).option("delimiter"," ").
    csv("../data/audioscrobbler/user_artist_data.txt").as[UserArtistData]
  val aliases = scala.io.Source.fromFile("../data/audioscrobbler/artist_alias.txt").getLines.flatMap { line =>
    val p = line.split("\t")
    try {
      Some(p(0).toInt -> p(1).toInt)
    } catch {
      case _: NumberFormatException => None
    }
  }.toMap
  val broadcastAliases = spark.sparkContext.broadcast(aliases)
  val dealiasedUserArtistData = userArtistData.map(uad => uad.copy(artistID = 
    broadcastAliases.value.getOrElse(uad.artistID, uad.artistID))).cache()
    
  val als = new ALS().setImplicitPrefs(true).setRank(10).setRegParam(0.1).setAlpha(1.0).setMaxIter(10).
    setUserCol("userID").setItemCol("artistID").setRatingCol("count")
  val model = als.fit(dealiasedUserArtistData)
  
  model.userFactors.show(false)
  model.itemFactors.show(false)
  
  val recommendations = model.recommendForAllUsers(5)
  recommendations.show()
  
  spark.stop()
}