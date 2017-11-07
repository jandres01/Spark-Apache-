package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.classification.GBTClassifier
import scala.io.Source._
import java.io._
import org.apache.spark.ml.evaluation.RegressionEvaluator

object sparkMlRecommend extends App {
  val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate()
  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)

  val columnSchema = StructType(Array(
    StructField("userID",IntegerType),
    StructField("rating",IntegerType),
    StructField("date",StringType),
    StructField("movieID",IntegerType)))

  val movieSchema = StructType(Array(
    StructField("movieID",IntegerType),
    StructField("yearRelease",IntegerType),
    StructField("title",StringType)))


  val mainData = scala.io.Source.fromFile("/data/BigData/Netflix/combined_data_1.txt").getLines //.mkString("\n")

  //Reading initial file and creating new File
//  val file = new File("/users/jandres/CSCI3395/InClass/movieRating.txt")
//  val bw = new BufferedWriter(new FileWriter(file))
//  
//  var movieID = ""
//  
//  val parseLine = mainData.foreach { lines =>
//    if(lines.contains(":")) {
//      movieID = lines.split(":")(0)
//    } else {
//      bw.write(lines +"," +movieID + "\n")//mainList.apply(movieCount -1) :+ lines
//    }
//  }
//  bw.close()

  val inClassData = spark.read.schema(columnSchema).option("header",false).option("delimiter", ",").csv("/users/jandres/CSCI3395/movieRating.txt").filter('movieID <= 1000 )
  val outClassTrainingData = spark.read.schema(columnSchema).option("header",false).option("delimiter", ",").csv("/users/jandres/CSCI3395/movieRating.txt").filter('movieID <= 1000 && 'userID <= 10000)
  val outClassTestingData = spark.read.schema(columnSchema).option("header",false).option("delimiter", ",").csv("/users/jandres/CSCI3395/movieRating.txt").filter('movieID <= 5000 && 'userID <= 100000)
  val movieData = spark.read.schema(movieSchema).option("header",false).option("delimiter", ",").csv("/data/BigData/Netflix/movie_titles.csv")

  //Q#1
//  val columnName = inClassData.columns(0)
//  
//  val rangeUserID = inClassData.agg(min(columnName),max(columnName))
//  rangeUserID.show()
//  
//  inClassData.describe("userID").show()
//  
//  //Q#2
//  val countDistinctUserID = inClassData.select('userID).distinct().count()
//  println("There are "+countDistinctUserID+" distinct userIDs")
//  
//  //Q#3
//  val c5StarRating1 = inClassData.filter('userID === 372233 && 'rating === 5).count()
//  println("User 372233 gave "+ c5StarRating1   + " 5 star rating")
//  
//  //Q#4
//  println("Movie with most user ratings")
//  val movieRatingCount = inClassData.groupBy('movieID).count()
//  
//  movieRatingCount.orderBy('count.desc).limit(1).show(false)
//  
//  movieData.filter('movieID === 571).show(false)
//  
////  val mostMovieRatings = movieRatingCount.agg(max("count"))
////  mostMovieRatings.show()
//  
//  //Q#5
//  println("Movie with most 5 star ratings")
//  inClassData.filter('rating === 5).groupBy('movieID).count().orderBy('count.desc).limit(1).show(false)
//  
//  movieData.filter('movieID === 571).show(false)
//  
  //Out of Class Q#1
  println("Top 5 Movie Recommendations for Users")
  val als = new ALS().setImplicitPrefs(false).setRank(10).setRegParam(0.1).setAlpha(1.0).setMaxIter(10).
    setUserCol("userID").setItemCol("movieID").setRatingCol("rating")
  val model = als.fit(outClassTestingData)

//  model.userFactors.show(false)
//  model.itemFactors.show(false)

  val recommendations = model.recommendForAllUsers(5)
  recommendations.orderBy('userID).show(false)

  println("Top 10 Most Commonly Recommended Movies for Users")

  val recommendedMovies = recommendations.select(explode($"recommendations").as("value"))
  val top10Recommended = recommendedMovies.withColumn("ids",recommendedMovies("value")("movieID")).groupBy('ids).count().orderBy($"count".desc).limit(10)
  val top10Titles = top10Recommended.join(movieData, top10Recommended("ids") === movieData("movieID"))

  top10Titles.select('movieID,'count,'title).show(false)

  //Q#2 Out of Class
  println("Quality of Movie Recommendations")

  val Array(training, test) = outClassTestingData.randomSplit(Array(0.8, 0.2))

  val model2 = als.fit(training)

  // Evaluate the model by computing the RMSE on the test data
  // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
  model2.setColdStartStrategy("drop")
  val predictions = model2.transform(test)

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions)
  println(s"Root-mean-square error = $rmse or machine learning is giving +/- $rmse rating from what users gave in actual data")

  // Generate top 10 movie recommendations for each user
  val userRecs = model2.recommendForAllUsers(10)
  userRecs.show(false)

  // Generate top 10 user recommendations for each movie
  //val movieRecs = model2.recommendForAllItems(10)


  spark.stop
}                                                                                                                                                                                                                                                                                                                                                                                                                                                      