package sparkml

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



object sparkmlclassify extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.OFF)
  
  val odata = spark.read.option("delimiter", "\t").csv("/data/BigData/admissions/AdmissionAnon.tsv")


  val dataRDD = spark.sparkContext.textFile("/data/BigData/admissions/AdmissionAnon.tsv")
  
  val schemeData = dataRDD.map { line =>
    line.split("\t")
  }
  
  //Q#1 How many Rows & columns
  val nColumn = schemeData.first().size
  val nRows = schemeData.count()
  
  println("number of Columns: "+ nColumn)
  println("number of Rows: "+ nRows)
  
  //Q#2 How many different values in last column'
 
  val arrLastColumn = schemeData.map(a => a(a.size-1))
  val distLColumn = arrLastColumn.distinct()
  println("Number of distinct values in last column "+distLColumn.count())
  
  //Q#3
  val numLast0 = arrLastColumn.filter(_ == "0").count()
  val numLast1 = arrLastColumn.filter(_ == "1").count()
  val numLast2 = arrLastColumn.filter(_ == "2").count()
  val numLast3 = arrLastColumn.filter(_ == "3").count()
  val numLast4 = arrLastColumn.filter(_ == "4").count()
  
  println("There are " + numLast0 + " rows with 0, " + numLast1 + " rows with 1, " + numLast2 + " rows with 2, " + numLast3 + " rows with 3, and " + numLast4 + " rows with 4.")
  
  //Q#4
  val numData = ((0 to 4) ++ (8 to 46)).map(i => odata.col("_c"+i).cast(DoubleType))
  val numArr = ((0 to 4) ++ (8 to 46)).toArray
  val dataSet = odata.select(numData:_*)
  val aData = dataSet.na.fill(-1.0)
  
  val featuresData = new VectorAssembler().setInputCols(aData.columns).setOutputCol("features")
  val assembledData = featuresData.transform(aData)
  
  //dataframe is a dataset of rows!!!
  val Row(correlMatrix: Matrix) = Correlation.corr(assembledData, "features").head
  println("Pearson correlation matrix:\n" + correlMatrix.toString)
  
  //Q#5
  def listCorrelations():ListBuffer[(Double,Int)] = {
    val listCorr = new ListBuffer[(Double,Int)]()
   
    for(i <- 0 until correlMatrix.numRows) {
      val value = math.abs(correlMatrix.apply(i, correlMatrix.numRows-1))
      if(value != 1.0 && value != -1.0) {
        listCorr += ((value,numArr(i)))
      }
    }
    listCorr
  }
  
  println(correlMatrix.numRows)
  
  val top3 = listCorrelations().sortBy(_._1).reverse.take(3)
  top3.map(i => println("Column in top 3 is "+ i._2 + " With absolute value correlation of "+ i._1))
  
  //Out of Class Q#1
  
  val numData2 = ((0 to 4) ++ (8 to 45)).map(i => ("_c"+i).toString()).toArray
  val fData = new VectorAssembler().setInputCols(numData2).setOutputCol("features")
  val lblDataRenamed = aData.withColumnRenamed("_c46" ,"label")
  
  val lblAssembledData = fData.transform(lblDataRenamed)
  
  val Array(trainData, testData) = lblAssembledData.randomSplit(Array(0.8, 0.2)).map(_.cache())
  
  val rf = new RandomForestClassifier
  
  //DT Classifier
  val labelIndexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("indexedLabel")
    .fit(lblAssembledData)
// Automatically identify categorical features, and index them.
  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
  // Train a DecisionTree model.
  val dt = new DecisionTreeClassifier()
    .setLabelCol("indexedLabel")
    .setFeaturesCol("indexedFeatures")
  // Convert indexed labels back to original labels.
  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(labelIndexer.labels)
  // Chain indexers and tree in a Pipeline.
  val pipeline = new Pipeline()
    .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
  
  // Train a GBT model.
  val gbt = new GBTClassifier()
    .setLabelCol("indexedLabel")
    .setFeaturesCol("indexedFeatures")
    .setMaxIter(10)
    
  val pipelineGBT = new Pipeline()
    .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))  
  
  val model = rf.fit(trainData)
  val predictions = model.transform(testData)
  
  val evaluator = new BinaryClassificationEvaluator
    
  val multiEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
   
  def listTopWeights(lst:Array[Double]):ListBuffer[(Double,Int)] = {
    val listCorr = new ListBuffer[(Double,Int)]()
   
    for(i <- 0 until lst.length) { 
        listCorr += ((lst(i),numArr(i)))
    }
    val top5 = listCorr.sortBy(_._1).reverse.take(5)
    top5
  }
    
  val accuracy = multiEvaluator.evaluate(predictions)
  println(s"RandomForestClassifier accuracy Out Of Class Q#1 = $accuracy")
  
  val arrWeightsR = model.featureImportances.toArray
  println("Top 5 Key Columns for RandomForestClassifier accuracy Out Of Class Q#1: ")
  listTopWeights(arrWeightsR).map(i => println(i._2))
  
  val model2 = pipeline.fit(trainData)
  val predictions2 = model2.transform(testData)
  val accuracy2 = multiEvaluator.evaluate(predictions2)
  println(s"DT accuracy Out Of Class Q#1 = $accuracy2")
  
//  val model3 = pipelineGBT.fit(trainData)
//  val predictions3 = model3.transform(testData)
//  val accuracy3 = multiEvaluator.evaluate(predictions3)
//  println(s"GBT accuracy OC Q#2 = $accuracy3")
  
  //Out of Class Q#2

  // Train model. This also runs the indexers.
//  val modelDT = pipeline.fit(trainData)
//  val predictionsBelow2DT = modelDT.transform(testData)
//  val accuracyBelow2DT = multiEvaluator.evaluate(predictionsBelow2DT)
//  println(s"DT accuracy for 2 Below = $accuracyBelow2DT")
  
  
  //Classify Below 2
  val lblDataBelow2 = aData.withColumn("label", when('_c46 < 2.0,1).otherwise(0))
  
  val lblADataBelow2 = fData.transform(lblDataBelow2)
  val Array(trainDataBelow2, testDataBelow2) = lblADataBelow2.randomSplit(Array(0.8, 0.2)).map(_.cache())
  
  
  val modelBelow2 = rf.fit(trainData)
  val predictionsBelow2 = modelBelow2.transform(testDataBelow2)
  val accuracyBelow2 = evaluator.evaluate(predictionsBelow2)
  println(s"RandomForestClassifier accuracy 2 Below OC Q#2 = $accuracyBelow2")
  
  val arrWeights = modelBelow2.featureImportances.toArray
  println("Top 5 Key Columns for Classification Below 2 OC Q#2: ")
  listTopWeights(arrWeights).map(i => println(i._2))
    //GBT only for binary classification  
//  val modelBelow2GBT = pipelineGBT.fit(trainDataBelow2)
//  val predictionsBelow2GBT = modelBelow2GBT.transform(testData)
//  val accuracyBelow2GBT = evaluator.evaluate(predictionsBelow2GBT)
//  println(s"GBT accuracy Below 2 OC Q#2 = $accuracyBelow2GBT")
  
  
  //Classify values 2 or more
  val lblDataAbove2 = aData.withColumn("label", when('_c46 >= 2.0,1).otherwise(0))
  val lblADataAbove2 = fData.transform(lblDataAbove2)
  val Array(trainDataAbove2, testDataAbove2) = lblADataAbove2.randomSplit(Array(0.8, 0.2)).map(_.cache())
  
  val modelAbove2 = rf.fit(trainDataAbove2)
  val predictionsAbove2 = modelAbove2.transform(testDataAbove2)
  val accuracyAbove2 = evaluator.evaluate(predictionsAbove2)
  println(s"RandomForestClassifier accuracy Above 2 OC Q#2 = $accuracyAbove2")
  
  val arrWeights2 = modelAbove2.featureImportances.toArray
  println("Top 5 Key Columns for Classification Above 2 OC Q#2: ")
  listTopWeights(arrWeights2).map(i => println(i._2))
  
//  val modelAbove2GBT = pipelineGBT.fit(trainDataAbove2)
//  val predictionsAbove2GBT = modelAbove2GBT.transform(testDataAbove2)
//  val accuracyAbove2GBT = evaluator.evaluate(predictionsAbove2GBT)
//  println(s"GBT accuracy above 2 OC Q#2 = $accuracyAbove2GBT")
  
}