package practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths

case class MarvelNode(name: String, ntype: String)

/**
 * Demonstrating the use of GraphX on the Marvel Social Network from https://www.kaggle.com/csanhueza/the-marvel-universe-social-network
 */
object MarvelGraph {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
		sc.setLogLevel("WARN")

    // Read in data
    val nodes = scala.io.Source.fromFile("/users/mlewis/CSCI3395-F17/data/nodes.csv").getLines.drop(1).map { line =>
      val comma = line.lastIndexOf(",")
      val (name, ntype) = line.trim.splitAt(comma)
      MarvelNode(name.filter(_ != '"'), ntype.drop(1))
    }.toArray.zipWithIndex.map { case (n, i) => i.toLong -> n }
    
    val indexMap = nodes.map { case (i, n) => n.name.take(20) -> i }.toMap
    
    val edges = scala.io.Source.fromFile("/users/mlewis/CSCI3395-F17/data/hero-network.csv").getLines.drop(1).flatMap { line =>
      val Array(n1, n2) = line.trim.split("\",\"")
      val (name1, name2) = (n1.drop(1).trim, n2.dropRight(1).trim)
      Seq(Edge(indexMap(name1), indexMap(name2), ()), Edge(indexMap(name2), indexMap(name1), ()))
    }.toArray
    
    // Make RDDs and graph
    val nodesRDD = sc.parallelize(nodes)
    val edgesRDD = sc.parallelize(edges)
    val graph = Graph(nodesRDD, edgesRDD)
    
    // Find distance between NIGHTCRAWLER | MUTAN and CAGE, LUKE/CARL LUCA
    val sp = ShortestPaths.run(graph, Seq(indexMap("CAGE, LUKE/CARL LUCA")))
    println(sp.vertices.filter(_._1==indexMap("NIGHTCRAWLER | MUTAN")).first) // -> x steps from nightcrawler to luke cage
    
    // Connected Components
    graph.connectedComponents().vertices.
        filter(t => t._2 != 3 && nodes(t._1.toInt)._2.ntype == "hero").collect.map(t => t._2 -> nodes(t._1.toInt)) foreach println  //nodes not part of primary component
    
    // Run page rank
        
    //heroRank only contain vertices that are heroes... epred = edge predicate, et = edge triplet 
    val heroGraph = graph.subgraph(et => {
      et.srcAttr.ntype == "hero" && et.dstAttr.ntype == "hero"
    }, (id, v) => v.ntype == "hero")
    val rankH = heroGraph.pageRank(0.01)
    val rankedH = nodesRDD.join(rankH.vertices).sortBy(-_._2._2)
    rankedH.take(20) foreach println  //because we removed so much nodes thus affected the ranking
        
        
    val ranks = graph.pageRank(0.01)
    val ranked = nodesRDD.join(ranks.vertices).sortBy(-_._2._2)
    //ranked.take(20) foreach println  //page ranks stop when ranks don't change by 0.01
    
    
    
    //large task size 
    
    sc.stop()
  }
}