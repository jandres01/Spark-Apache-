package practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object CensusDataClassify extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val schema = StructType(Array(
    StructField("age", IntegerType),
    StructField("workclass", StringType),
    StructField("fnlwgt", IntegerType),
    StructField("education", StringType),
    StructField("educationNum", IntegerType),
    StructField("maritalStatus", StringType),
    StructField("occupation", StringType),
    StructField("relationship", StringType),
    StructField("race", StringType),
    StructField("sex", StringType),
    StructField("capitalGain", IntegerType),
    StructField("capitalLoss", IntegerType),
    StructField("hoursPerWeek", IntegerType),
    StructField("nativeCountry", StringType),
    StructField("income", StringType)))
    
  val data = spark.read.schema(schema).option("header",true).csv("/users/mlewis/CSCI3395-F17/data/adult.csv").cache()
  
  val intFeatureCols = "age fnlwgt educationNum capitalGain capitalLoss hoursPerWeek".split(" ")
  val stringFeatureCols = "workclass education maritalStatus occupation relationship race sex".split(" ")
  
  val indexedData = stringFeatureCols.foldLeft(data) {(ds, name) =>
    val indexer = new StringIndexer().setInputCol(name).setOutputCol(name+"-i")
    indexer.fit(ds).transform(ds)
  }.withColumn("label", when('income === ">50K", 1).otherwise(0))
  //indexedData.show()
  
  val assembler = new VectorAssembler().
    setInputCols(intFeatureCols ++ stringFeatureCols.map(_+"-i")).
    setOutputCol("features")
  val assembledData = assembler.transform(indexedData)
  assembledData.show()
  
  val Array(trainData, testData) = assembledData.randomSplit(Array(0.7, 0.3)).map(_.cache())
  
  val rf = new RandomForestClassifier
  val model = rf.fit(trainData)
  
  val predictions = model.transform(testData)
  predictions.show()
  val evaluator = new BinaryClassificationEvaluator
  val accuracy = evaluator.evaluate(predictions)
  println(s"accuracy = $accuracy")

  spark.stop
}