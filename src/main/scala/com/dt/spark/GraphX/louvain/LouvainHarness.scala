package com.dt.spark.GraphX.louvain

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.graphstream.graph.{Graph => GraphStream}
import org.graphstream.graph.implementations._
/**
 * Coordinates execution of the louvain distributed community detection process on a graph.
 * 
 * The input Graph must have an edge type of Long.
 * 
 * All lower level algorithm functions are in LouvainCore, this class acts to 
 * coordinate calls into LouvainCore and check for convergence criteria
 * 
 * Two hooks are provided to allow custom behavior
 *    -saveLevel  override to save the graph (vertcies/edges) after each phase of the process 
 *    -finalSave  override to specify a final action / save when the algorithm has completed. (not nessicary if saving at each level)
 * 
 * High Level algorithm description.
 *  
 *  Set up - Each vertex in the graph is assigned its own community.
 *  1.  Each vertex attempts to increase graph modularity by changing to a neighboring community, or reamining in its current community.
 *  2.  Repeat step 1 until progress is no longer made 
 *         - progress is measured by looking at the decrease in the number of vertices that change their community on each pass.
 *           If the change in progress is < minProgress more than progressCounter times we exit this level.
 *  3. -saveLevel, each vertex is now labeled with a community.
 *  4. Compress the graph representing each community as a single node.
 *  5. repeat steps 1-4 on the compressed graph.
 *  6. repeat until modularity is no longer improved
 *  
 *  For details see:  Fast unfolding of communities in large networks, Blondel 2008
 *  
 *  
 */
class  LouvainHarness(minProgress:Int,progressCounter:Int) {

  
  def run[VD: ClassTag](sc:SparkContext,graph:Graph[VD,(Long,Long)]) = {
    
    var louvainGraph = LouvainCore.createLouvainGraph(graph)
    var level = -1  // number of times the graph has been compressed
	  var q = -1.0    // current modularity value
	  var qNoWeight = -1.0
	  var halt = false
	  // this is a new feature to record the times of the iteration
	  var iteration=1
	  
    do {
	  level += 1
	  println(s"\nStarting Louvain level $level")
	//  val tmp1=louvainGraph.vertices.collect()
  //	tmp1.foreach(println)
	  // label each vertex with its best community choice at this level of compression
	  if (iteration==1)
	  {
  	  val (currentQ,currentGraph,passes) = LouvainCore.louvain(sc, louvainGraph,minProgress,progressCounter)
  	  val temp=currentGraph.vertices.foreach(println)
  	  louvainGraph.unpersistVertices(blocking=false)
  	  louvainGraph=currentGraph
  	  var currentQ2=currentQ
  	  val (currentQ1,currentGraph1) = LouvainCore.trimVertices(sc, louvainGraph)
  	  currentGraph1.vertices.foreach(println)
  	  
  	  louvainGraph.unpersistVertices(blocking=false)
  	  louvainGraph=currentGraph1
  	  var currentQ3=currentQ1
  	  while(currentQ2!=currentQ3)
  	  {
  	    currentQ2= currentQ3
  	    val (currentQ1,currentGraph1) = LouvainCore.trimVertices(sc, louvainGraph)
  	     currentGraph1.vertices.foreach(println)
  	    louvainGraph.unpersistVertices(blocking=false)
  	    louvainGraph=currentGraph1
  	    currentQ3=currentQ1
  	  }
  	 // val tmp=louvainGraph.vertices.collect()
  	 // tmp.foreach(println)
  	  //saveLevel(sc,level,currentQ,louvainGraph)
  	  if (passes > 1 && currentQ > q + 0.001 ){ 
	        q = currentQ
	        louvainGraph = LouvainCore.compressGraph(louvainGraph)      
	      }
	    else {
	      halt = true
	      }
	  }
	  else
	  {
  	    val (currentQ,currentGraph,passes) = LouvainCore.louvainNoWeight(sc, louvainGraph,minProgress,progressCounter)
  	    louvainGraph.unpersistVertices(blocking=false)
  	    louvainGraph=currentGraph
  	      val tmp=louvainGraph.vertices.collect()
  	      tmp.foreach(println)
  	    //saveLevel(sc,level,currentQ,louvainGraph)
  	    if (passes > 1 && currentQ > qNoWeight + 0.001 ){ 
	        qNoWeight = currentQ
	        louvainGraph = LouvainCore.compressGraph(louvainGraph)
	        }
	      else {
	        halt = true
	      }
	  }
	  
	  
//	System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")
	  
	  
	/*val graphStream:SingleGraph = new SingleGraph("GraphStream")

  // 设置graphStream全局属性. Set up the visual attributes for graph visualization
  graphStream.addAttribute("ui.stylesheet","url(file:///home/zhoukun/Desktop/stylesheet.css)")
  graphStream.addAttribute("ui.quality")
  graphStream.addAttribute("ui.antialias")


  // 加载顶点到可视化图对象中
  for ( (vid,vertextstate) <- louvainGraph.vertices.collect()) {
    val node = graphStream.addNode(vid.toString).asInstanceOf[SingleNode]
    node.addAttribute(vertextstate.community.toString())
  }
  //加载边到可视化图对象中
  for (Edge(x,y,value) <- louvainGraph.edges.collect()) {
    val edge = graphStream.addEdge(x.toString ++ y.toString,
      x.toString, y.toString,
      true).
      asInstanceOf[AbstractEdge]

  }
  //显示
  graphStream.display()*/
	  
	  
	  
	  // If modularity was increased by at least 0.001 compress the graph and repeat
	  // halt immediately if the community labeling took less than 3 passes
	  //println(s"if ($passes > 2 && $currentQ > $q + 0.001 )")

	 iteration=iteration+1
	}while ( !halt )
	finalSave(sc,level,q,louvainGraph)  
  }

  /**
   * Save the graph at the given level of compression with community labels
   * level 0 = no compression
   * 
   * override to specify save behavior
   */
  def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,(Long,Long)]) = {
    
  }
  
  /**
   * Complete any final save actions required
   * 
   * override to specify save behavior
   */
  def finalSave(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,(Long,Long)]) = {
    
  }
  
  
  
}