package sparkgraphx

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.Row
import practice.MarvelNode
import swiftvis2.plotting.Plot
import swiftvis2.plotting.renderer.FXRenderer
import swiftvis2.plotting.renderer._
import scalafx.application.JFXApp
import org.apache.spark.rdd.PairRDDFunctions
import swiftvis2.plotting._
import org.apache.spark.rdd.RDD

/**
 * Demonstrating the use of GraphX on the Marvel Social Network from https://www.kaggle.com/csanhueza/the-marvel-universe-social-network
 */
object graphx extends JFXApp {

    //Logger.getLogger("org").setLevel(Level.OFF)  
    
    val conf = new SparkConf().setAppName("Graph Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
		sc.setLogLevel("WARN")
		

		val data = xml.XML.loadFile("/data/BigData/Medline/medsamp2016a.xml")
		val descName = (data \ "MedlineCitation" \\ "DescriptorName").map(_.text)
		
		def grabDescName(x:String) = {
      val addingData = xml.XML.loadFile("/data/BigData/Medline/medsamp2016"+x+".xml")
		  val addingDescName = (addingData \ "MedlineCitation" \\ "DescriptorName").map(_.text)
		  addingDescName
    }
		
		val totalDescName = descName ++ grabDescName("b") ++ grabDescName("c") ++ grabDescName("d") ++ grabDescName("e") ++ grabDescName("f") ++ grabDescName("g") ++ grabDescName("h")
		
		//In Class Q#1
		println("Number of Distinct Names")
		println(totalDescName.distinct.count(_ => true))
    
		//In Class Q#2
		println("Top 10 most common DescriptorNames")
		val nodeRDD = sc.parallelize(totalDescName)
		val commonNames = nodeRDD.countByValue
		val commonRDD = sc.parallelize(commonNames.toSeq).sortBy(_._2, ascending=false)
		commonRDD.take(10) foreach println

		//In Class Q#3
		println("Top 10 Most common Major Descriptor Names")
		
	  def grabMajorDescName(x:String) = {
      val addingData = xml.XML.loadFile("/data/BigData/Medline/medsamp2016"+x+".xml")
		  val addingMajorDescName = (addingData \ "MedlineCitation" \\ "DescriptorName").filter(x => (x \ "@MajorTopicYN").toString =="Y").map(_.text)
		  addingMajorDescName
    }
		
		val descMajorName = (data \ "MedlineCitation" \\ "DescriptorName").filter(x => x.attributes("MajorTopicYN").toString() == "Y").map(_.text)
		
	  val totalMajorDescName = descMajorName ++ grabMajorDescName("b") ++ grabMajorDescName("c") ++ grabMajorDescName("d") ++ grabMajorDescName("e") ++ grabMajorDescName("f") ++ grabMajorDescName("g") ++ grabMajorDescName("h")

	  val nodesMajorRDD = sc.parallelize(totalMajorDescName)
		val commonMajorNames = nodesMajorRDD.countByValue
		val commonMajorRDD = sc.parallelize(commonMajorNames.toSeq).sortBy(_._2, ascending=false)
		commonMajorRDD.take(10) foreach println

		//In Class Q#4
		println("Total Number of Pairs with Distinct Descriptor Names")
		val distinctDescName = totalMajorDescName.distinct
	  val distinctRDD = sc.parallelize(distinctDescName)
		val totalCountPairDescriptorNames = (distinctRDD.count() * (distinctRDD.count() -1))/2
    println(totalCountPairDescriptorNames)
		
		//In Class Q#5
    println("Number of Actual Pair Descriptor Names")
//    val numberPairDescriptorNames = distinctRDD.flatMap(list => list.combinations(2))
//    println(numberPairDescriptorNames.count())
    
//    def grabNodes(x:String) = {
//      val addingData = xml.XML.loadFile("/data/BigData/Medline/medsamp2016"+x+".xml")
//		  val addingNodes = (addingData \ "MedlineCitation").map { nodes =>
//		    (nodes \\ "DescriptorName").map(_.text)
//      }
//		  addingNodes
//    }
//    
//    val totalNodes = grabNodes("a") ++ grabNodes("b") ++ grabNodes("c") ++ grabNodes("d") ++ grabNodes("e") ++ grabNodes("f") ++ grabNodes("g") ++ grabNodes("h")
//    
//    val edges = totalNodes flatMap { seq =>
//      seq.combinations(2)
//    }
    
    def grabMajorNodes(x:String) = {
      val addingData = xml.XML.loadFile("/data/BigData/Medline/medsamp2016"+x+".xml")
		  val addingNodes = (addingData \ "MedlineCitation").map { nodes =>
		    (nodes \\ "DescriptorName").filter(x => x.attributes("MajorTopicYN").toString() == "Y").map(_.text)
      }
		  addingNodes
    }
    
    val totalMajorNodes = grabMajorNodes("a") ++ grabMajorNodes("b") ++ grabMajorNodes("c") ++ grabMajorNodes("d") ++ grabMajorNodes("e") ++ grabMajorNodes("f") ++ grabMajorNodes("g") ++ grabMajorNodes("h")
    
    val majorEdges = totalMajorNodes flatMap { seq =>
      seq.combinations(2)
    }

    println(majorEdges.distinct.count(_ => true))     
    
    //Out of Class
    val nodes = distinctDescName.zipWithIndex.map { case (n, i) => i.toLong -> n }
    println("wierd") 
    
    val indexMap = nodes.map { case (i, n) => n -> i }.toMap
    println("edges")
    val edges2 = majorEdges.flatMap { seq =>
      Seq(Edge(indexMap(seq(0)), indexMap(seq(1)), ()), 
          Edge(indexMap(seq(1)), indexMap(seq(0)), ()) )
    }.toArray
    println("why")
    val nodesRDD = sc.parallelize(nodes)
    val edgesRDD = sc.parallelize(edges2)
    println("hi")
    val graph = Graph(nodesRDD, edgesRDD)
    
    println(graph.numEdges)
    
    //Out of Class Q#1
        
    println("Connected Components")
    // finds Connected Components and collects them
    val countComponents = graph.connectedComponents().vertices.map(t => t._2).distinct.count()
    
    println("Number of connected components = " +countComponents)
    
    // Out Of Class Q#2 - page rank
    val ranks = graph.pageRank(0.01)
    val ranked = nodesRDD.join(ranks.vertices).sortBy(_._2._2, false)
    ranked.take(20) foreach println //page ranks stop when ranks don't change by 0.01

    //Out of Class Q#3 - Histogram
    println("Histogram")
    val degreesRDD = graph.degrees.sortBy(_._1)

    val arrDegrees = degreesRDD.map(_._2)
    
    val bin = (0.0 to degreesRDD.map(_._1.toDouble).max() by 11).toArray
    val biny = degreesRDD.map(_._1.toDouble).collect()
    val hist = arrDegrees.histogram(biny, true)
    val plot = Plot.histogramPlot(bin, hist, BlueARGB, true, "Degree Distribution Histogram", "ID", "Degrees")
    FXRenderer(plot)

    //Out of Class Q#4
//    println("Shortest Paths")
//    //Find distance between Pregnancy and Esophagus
//    val sp = ShortestPaths.run(graph, Seq(indexMap("Pregnancy")))
//    println(sp.vertices.filter(_._1==indexMap("Esophagus")).first)
//    
//    //shortestPath Femoral Artery and Electroencephalography
//    val sp3 = ShortestPaths.run(graph, Seq(indexMap("Femoral Artery")))
//    println(sp3.vertices.filter(_._1==indexMap("Electroencephalography")).first)
//    
//    //shortestPath Taxes and Guinea Pigs
//    
//    val sp6 = ShortestPaths.run(graph, Seq(indexMap("Taxes")))
//    println(sp6.vertices.filter(_._1==indexMap("Guinea Pigs")).first)
    
    
    sc.stop()

}