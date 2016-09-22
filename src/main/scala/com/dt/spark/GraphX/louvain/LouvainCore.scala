package com.dt.spark.GraphX.louvain

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.math.BigDecimal.double2bigDecimal



/**
 * Provides low level louvain community detection algorithm functions.  Generally used by LouvainHarness
 * to coordinate the correct execution of the algorithm though its several stages.
 * 
 * For details on the sequential algorithm see:  Fast unfolding of communities in large networks, Blondel 2008
 */
object LouvainCore {

  
  
   /**
    * Generates a new graph of type Graph[VertexState,Long] based on an input graph of type.
    * Graph[VD,(Long,Long)].  The resulting graph can be used for louvain computation.
    * 
    */
  ///////////////////////////done///////////////////////////////////////////////
  // changed by zhoukun 2016-9-20
   def createLouvainGraph[VD: ClassTag](graph: Graph[VD,(Long,Long)]) : Graph[VertexState,(Long,Long)]= {
    // Create the initial Louvain graph.  
    val nodeWeightMapFunc = (e:EdgeTriplet[VD,(Long,Long)]) => Iterator((e.srcId,e.attr), (e.dstId,e.attr))
    val nodeWeightReduceFunc = (e1:(Long,Long),e2:(Long,Long)) => (e1._1+e2._1,e1._2+e2._2)
    val nodeWeights = graph.mapReduceTriplets(nodeWeightMapFunc,nodeWeightReduceFunc)
    val merge=(e1:(Long,Long),e2:(Long,Long)) => (e1._1+e2._1,e1._2+e2._2)
    //nodeWeights.foreach{case (vid,value)=> println(s" $vid:$value")}
    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
      val weight = weightOption.getOrElse((0L,0L))
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = weight._1
      state.internalWeight = 0L
      state.nodeWeight = weight._1
      // this is the new feature of the new graph without the initial weight
      // changed at 2016-9-20 by zhoukun
      state.nodeWeightNoWeight=weight._2
      state.communitySigmaTotNoWeight=weight._2
      state.internalWeightNoWeight=0L
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(merge)
    return louvainGraph
   }
  /////////////////////////////////////////////////////////////////////////////////////
    
   
  /**
   * Transform a graph from [VD,Long] to a a [VertexState,Long] graph and label each vertex with a community
   * to maximize global modularity (without compressing the graph)
   */
  def louvainFromStandardGraph[VD: ClassTag](sc:SparkContext,graph:Graph[VD,(Long,Long)], minProgress:Int=1,progressCounter:Int=1) : (Double,Graph[VertexState,(Long,Long)],Int) = {
	  val louvainGraph = createLouvainGraph(graph)
	  return louvain(sc,louvainGraph,minProgress,progressCounter)
  }
  
  
  
  /**
   * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize global modularity. 
   * (without compressing the graph)
   */
  def louvain(sc:SparkContext, graph:Graph[VertexState,(Long,Long)], minProgress:Int=1,progressCounter:Int=1) : (Double,Graph[VertexState,(Long,Long)],Int)= {
    var louvainGraph = graph.cache()
    // calculate the graph weight with and without the initial weight
    val graphWeight = louvainGraph.vertices.values.map(vdata=> vdata.internalWeight+vdata.nodeWeight).reduce(_+_)
    val temp=louvainGraph.vertices.values.collect();
    var totalGraphWeight = sc.broadcast(graphWeight) 
    println("totalEdgeWeight: "+totalGraphWeight.value)
    
    val graphWeightNoWeight = louvainGraph.vertices.values.map(vdata=> vdata.internalWeightNoWeight+vdata.nodeWeightNoWeight).reduce(_+_)
     
    var totalGraphWeightNoWeight = sc.broadcast(graphWeightNoWeight) 
    println("totalEdgeWeight: "+totalGraphWeightNoWeight.value)
    
    
    // gather community information from each vertex's local neighborhood
    var msgRDD = louvainGraph.mapReduceTriplets(sendMsg,mergeMsg)
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory
   // val tmp=msgRDD.first().toString()
   // println(tmp)
    var updated = 0L - minProgress
    var even = false  
    var count = 0
    val maxIter = 100000 
    var stop = 0
    var updatedLastPhase = 0L
    do { 
       count += 1
	   even = ! even	   
	  
	   // label each vertex with its best community based on neighboring community information
	   val labeledVerts = louvainVertJoin(louvainGraph,msgRDD,totalGraphWeight,even).cache()  // chage the commnunity  sigmatot   changed lebel
	    
	     
	   // calculate new sigma total value for each community (total weight of each community)
	   val communtiyUpdate = labeledVerts
	    .map( {case (vid,vdata) => (vdata.community,vdata.nodeWeight+vdata.internalWeight)})
	   .reduceByKey(_+_).cache()
	   
	   //calculate the new sigma total without initial weight for each community
	   val communtiyUpdateNoWeight = labeledVerts
	    .map( {case (vid,vdata) => (vdata.community,vdata.nodeWeightNoWeight+vdata.internalWeightNoWeight)})
	   .reduceByKey(_+_).cache()
	   
	   
	   // map each vertex ID to its updated community information
	   val communityMapping = labeledVerts
	   .map( {case (vid,vdata) => (vdata.community,vid)})
	     .join(communtiyUpdate).join(communtiyUpdateNoWeight)
	     .map({case (community,((vid,sigmaTot),sigmaTotNoWeight)) => (vid,(community,sigmaTot,sigmaTotNoWeight)) })
	   .cache()
	   
	   // join the community labeled vertices with the updated community info
	   val updatedVerts = labeledVerts.join(communityMapping).map({ case (vid,(vdata,communityTuple) ) => 
	     vdata.community = communityTuple._1  
	     vdata.communitySigmaTot = communityTuple._2
	     vdata.communitySigmaTotNoWeight=communityTuple._3
	     (vid,vdata)
	   }).cache()
	   updatedVerts.count()
	   labeledVerts.unpersist(blocking = false)
	   communtiyUpdate.unpersist(blocking=false)
	   communtiyUpdateNoWeight.unpersist(blocking=false)
	   communityMapping.unpersist(blocking=false)
	   ////////////////till now all the parameters are chaged no matter the initial weight or the noweigth
	   val prevG = louvainGraph
	   louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
	   louvainGraph.cache()
	   
       // gather community information from each vertex's local neighborhood
	   val oldMsgs = msgRDD
       msgRDD = louvainGraph.mapReduceTriplets(sendMsg, mergeMsg).cache()
       activeMessages = msgRDD.count()  // materializes the graph by forcing computation
	 
       oldMsgs.unpersist(blocking=false)
       updatedVerts.unpersist(blocking=false)
       prevG.unpersistVertices(blocking=false)
       
       // half of the communites can swtich on even cycles
       // and the other half on odd cycles (to prevent deadlocks)
       // so we only want to look for progess on odd cycles (after all vertcies have had a chance to move)
	   if (even) updated = 0
	   updated = updated + louvainGraph.vertices.filter(_._2.changed).count 
	   if (!even) {
	     println("  # vertices moved: "+java.text.NumberFormat.getInstance().format(updated))
	     if (updated >= updatedLastPhase - minProgress) stop += 1
	     updatedLastPhase = updated
	   }

   
    } while ( stop <= progressCounter && (even ||   (updated > 0 && count < maxIter)))
    println("\nCompleted in "+count+" cycles")
   
   
    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid,vdata,msgs)=> {
        // sum the nodes internal weight and all of its edges that are in its community
        val community = vdata.community
        var k_i_in = vdata.internalWeight
        var sigmaTot = vdata.communitySigmaTot.toDouble
        msgs.foreach({ case((communityId,sigmaTotal),communityEdgeWeight ) => 
          if (vdata.community == communityId) k_i_in += communityEdgeWeight})
        val M = totalGraphWeight.value
        val k_i = vdata.nodeWeight + vdata.internalWeight
        var q = (k_i_in.toDouble / M) -  ( ( sigmaTot *k_i) / math.pow(M, 2) )
        //println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
        if (q < 0) 0 else q
    })  
    
   //val te=newVerts.collect()
    val actualQ = newVerts.values.reduce(_+_)
    
    // return the modularity value of the graph along with the 
    // graph. vertices are labeled with their community
    return (actualQ,louvainGraph,count/2)
   
  }
  //B C 3 B A B C C A A
// this is a new function which calculate the graph with no initial weight all the parameters are with the appendix "NoWeight"
 // CHANGED AT 2016-9-20
  def louvainNoWeight(sc:SparkContext, graph:Graph[VertexState,(Long,Long)], minProgress:Int=1,progressCounter:Int=1) : (Double,Graph[VertexState,(Long,Long)],Int)= {
     var louvainGraph = graph.cache()
    // calculate the graph weight with and without the initial weight
    val graphWeight = louvainGraph.vertices.values.map(vdata=> vdata.internalWeight+vdata.nodeWeight).reduce(_+_)
    val temp=louvainGraph.vertices.values.collect();
    var totalGraphWeight = sc.broadcast(graphWeight) 
    println("totalEdgeWeight: "+totalGraphWeight.value)
    
    val graphWeightNoWeight = louvainGraph.vertices.values.map(vdata=> vdata.internalWeightNoWeight+vdata.nodeWeightNoWeight).reduce(_+_)
    var totalGraphWeightNoWeight = sc.broadcast(graphWeightNoWeight) 
    println("totalEdgeWeight: "+totalGraphWeightNoWeight.value)
    
    
    // gather community information from each vertex's local neighborhood
    var msgRDD = louvainGraph.mapReduceTriplets(sendMsgNoWeight,mergeMsgNoWeight)
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory
   // val tmp=msgRDD.first().toString()
   // println(tmp)
    var updated = 0L - minProgress
    var even = false  
    var count = 0
    val maxIter = 100000 
    var stop = 0
    var updatedLastPhase = 0L
    do { 
       count += 1
	   even = ! even	   
	  
	   // label each vertex with its best community based on neighboring community information
	   msgRDD.foreach(println)
	   louvainGraph.edges.foreach(println)
	   
	   val labeledVerts = louvainVertJoinNoWeight(louvainGraph,msgRDD,totalGraphWeightNoWeight,even).cache()  
	    
	   labeledVerts.foreach(println)
	   // calculate new sigma total value for each community (total weight of each community)
	   val communtiyUpdate = labeledVerts
	    .map( {case (vid,vdata) => (vdata.community,vdata.nodeWeight+vdata.internalWeight)})
	   .reduceByKey(_+_).cache()
	   
	   //calculate the new sigma total without initial weight for each community
	   val communtiyUpdateNoWeight = labeledVerts
	    .map( {case (vid,vdata) => (vdata.community,vdata.nodeWeightNoWeight+vdata.internalWeightNoWeight)})
	   .reduceByKey(_+_).cache()
	   
	   
	   // map each vertex ID to its updated community information
	   val communityMapping = labeledVerts
	   .map( {case (vid,vdata) => (vdata.community,vid)})
	     .join(communtiyUpdate).join(communtiyUpdateNoWeight)
	     .map({case (community,((vid,sigmaTot),sigmaTotNoWeight)) => (vid,(community,sigmaTot,sigmaTotNoWeight)) })
	   .cache()
	   
	   // join the community labeled vertices with the updated community info
	   val updatedVerts = labeledVerts.join(communityMapping).map({ case (vid,(vdata,communityTuple) ) => 
	     vdata.community = communityTuple._1  
	     vdata.communitySigmaTot = communityTuple._2
	     vdata.communitySigmaTotNoWeight=communityTuple._3
	     (vid,vdata)
	   }).cache()
	   updatedVerts.count()
	   labeledVerts.unpersist(blocking = false)
	   communtiyUpdate.unpersist(blocking=false)
	   communtiyUpdateNoWeight.unpersist(blocking=false)
	   communityMapping.unpersist(blocking=false)
	   
	   val prevG = louvainGraph
	   louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
	   louvainGraph.cache()
	   
       // gather community information from each vertex's local neighborhood
	   val oldMsgs = msgRDD
       msgRDD = louvainGraph.mapReduceTriplets(sendMsgNoWeight, mergeMsgNoWeight).cache()
       activeMessages = msgRDD.count()  // materializes the graph by forcing computation
	 
       oldMsgs.unpersist(blocking=false)
       updatedVerts.unpersist(blocking=false)
       prevG.unpersistVertices(blocking=false)
       
       // half of the communites can swtich on even cycles
       // and the other half on odd cycles (to prevent deadlocks)
       // so we only want to look for progess on odd cycles (after all vertcies have had a chance to move)
	   if (even) updated = 0
	   updated = updated + louvainGraph.vertices.filter(_._2.changed).count 
	   if (!even) {
	     println("  # vertices moved: "+java.text.NumberFormat.getInstance().format(updated))
	     if (updated >= updatedLastPhase - minProgress) stop += 1
	     updatedLastPhase = updated
	   }

   
    } while ( stop <= progressCounter && (even ||   (updated > 0 && count < maxIter)))
    println("\nCompleted in "+count+" cycles")
   
   
    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid,vdata,msgs)=> {
        // sum the nodes internal weight and all of its edges that are in its community
        val community = vdata.community
        var k_i_in = vdata.internalWeightNoWeight
        var sigmaTot = vdata.communitySigmaTotNoWeight.toDouble
        msgs.foreach({ case((communityId,sigmaTotal),communityEdgeWeight ) => 
          if (vdata.community == communityId) k_i_in += communityEdgeWeight})
        val M = totalGraphWeight.value
        val k_i = vdata.nodeWeightNoWeight + vdata.internalWeightNoWeight
        var q = (k_i_in.toDouble / M) -  ( ( sigmaTot *k_i) / math.pow(M, 2) )
        //println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
        if (q < 0) 0 else q
    })  
    
   //val te=newVerts.collect()
    val actualQ = newVerts.values.reduce(_+_)
    
    // return the modularity value of the graph along with the 
    // graph. vertices are labeled with their community
    return (actualQ,louvainGraph,count/2)
   
  }
  
  
 
  
  ////////////////////////////////done///////////////////////////////////////////////////
  /**
   * Creates the messages passed between each vertex to convey neighborhood community data.
   */
  private def sendMsg(et:EdgeTriplet[VertexState,(Long,Long)]) = {
    val m1 = (et.dstId,Map((et.srcAttr.community,et.srcAttr.communitySigmaTot)->et.attr._1))
	val m2 = (et.srcId,Map((et.dstAttr.community,et.dstAttr.communitySigmaTot)->et.attr._1))
	Iterator(m1, m2)    
  }
  
   
  /**
   *  Merge neighborhood community data into a single message for each vertex
   */
  private def mergeMsg(m1:Map[(Long,Long),Long],m2:Map[(Long,Long),Long]) ={
    val newMap = scala.collection.mutable.HashMap[(Long,Long),Long]()
    m1.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }
  ////////////////////////////////////////////////////////////////////////////////////////////////
  
  /////////////////////////////////////////////done///////////////////////////////////////////////
  private def sendMsgNoWeight(et:EdgeTriplet[VertexState,(Long,Long)]) = {
    val m1 = (et.dstId,Map((et.srcAttr.community,et.srcAttr.communitySigmaTotNoWeight)->et.attr._2))
	val m2 = (et.srcId,Map((et.dstAttr.community,et.dstAttr.communitySigmaTotNoWeight)->et.attr._2))
	Iterator(m1, m2)    
  }
  
    
  /**
   *  Merge neighborhood community data into a single message for each vertex
   */
  private def mergeMsgNoWeight(m1:Map[(Long,Long),Long],m2:Map[(Long,Long),Long]) ={
    val newMap = scala.collection.mutable.HashMap[(Long,Long),Long]()
    m1.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }
  
  /////////////////////////////////////////////////////////////////////////////////////////////////
  
  
  
  
   /**
   * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
   * Returns a new set of vertices with the updated vertex state.
   */
  private def louvainVertJoin(louvainGraph:Graph[VertexState,(Long,Long)], msgRDD:VertexRDD[Map[(Long,Long),Long]], totalEdgeWeight:Broadcast[Long], even:Boolean) = {
     louvainGraph.vertices.innerJoin(msgRDD)( (vid, vdata, msgs)=> {
	    var bestCommunity = vdata.community
		  var startingCommunityId = bestCommunity
		  var maxDeltaQ = BigDecimal(0.0);
	      var bestSigmaTot = 0L
	    //println(s"this is a test for the vadata...        $vid")
	      msgs.foreach({ case( (communityId,sigmaTotal),communityEdgeWeight ) => 
	      	val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight,totalEdgeWeight.value)
	    // println("   communtiy: "+communityId+" sigma:"+sigmaTotal+" edgeweight:"+communityEdgeWeight+"  q:"+deltaQ)
	        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))){
	          maxDeltaQ = deltaQ
	          bestCommunity = communityId
	          bestSigmaTot = sigmaTotal
	        }
	      })	      
	      // only allow changes from low to high communties on even cyces and high to low on odd cycles
		  if ( vdata.community != bestCommunity && ( (even && vdata.community > bestCommunity)  || (!even && vdata.community < bestCommunity)  )  ){
		 //   println("  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
		    vdata.community = bestCommunity
		    vdata.communitySigmaTot = bestSigmaTot  
		    vdata.changed = true
		   // println(vdata.toString())
		  }
		  else{
		    vdata.changed = false
		  }   
	     vdata
	   })
  }
  // this is a funciton used with the NoWeight parameters
  // changed by zhoukun in 2016-9-20
  private def louvainVertJoinNoWeight(louvainGraph:Graph[VertexState,(Long,Long)], msgRDD:VertexRDD[Map[(Long,Long),Long]], totalEdgeWeightNoWeight:Broadcast[Long], even:Boolean) = {
     louvainGraph.vertices.innerJoin(msgRDD)( (vid, vdata, msgs)=> {
	    var bestCommunity = vdata.community
		  var startingCommunityId = bestCommunity
		  var maxDeltaQ = BigDecimal(0.0);
	    var bestSigmaTot = 0L
	    //println(s"this is a test for the vadata...        $vid")
	      msgs.foreach({ case( (communityId,sigmaTotal),communityEdgeWeight ) => 
	      	val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeightNoWeight, vdata.internalWeightNoWeight,totalEdgeWeightNoWeight.value)
	       println("   communtiy: "+communityId+" sigma:"+sigmaTotal+" edgeweight:"+communityEdgeWeight+"  q:"+deltaQ)
	        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))){
	          maxDeltaQ = deltaQ
	          bestCommunity = communityId
	          bestSigmaTot = sigmaTotal
	        }
	      })	      
	      // only allow changes from low to high communties on even cyces and high to low on odd cycles
		  if ( vdata.community != bestCommunity && ( (even && vdata.community > bestCommunity)  || (!even && vdata.community < bestCommunity)  )  ){
		 //   println("  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
		    vdata.community = bestCommunity
		    vdata.communitySigmaTotNoWeight = bestSigmaTot  
		    vdata.changed = true
		   // println(vdata.toString())
		  }
		  else{
		    vdata.changed = false
		  }   
	     vdata
	   })
  }
  
  /**
   * Returns the change in modularity that would result from a vertex moving to a specified community.
   */
  // this fucntion is not need to be changed because it is enough when just change the parameters
  private def q(currCommunityId:Long, testCommunityId:Long, testSigmaTot:Long, edgeWeightInCommunity:Long, nodeWeight:Long, internalWeight:Long, totalEdgeWeight:Long) : BigDecimal = {
	  	val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
		val M = BigDecimal(totalEdgeWeight); 
	  	val k_i_in_L =  if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
		val k_i_in = BigDecimal(k_i_in_L);
		val k_i = BigDecimal(nodeWeight + internalWeight);
		val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot);
		
		var deltaQ =  BigDecimal(0.0);
		if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
			deltaQ = k_i_in - ( k_i * sigma_tot / M)
			//println(s"      $deltaQ = $k_i_in - ( $k_i * $sigma_tot / $M")		
		}
		return deltaQ;
  }
 

  
  /**
   * Compress a graph by its communities, aggregate both internal node weights and edge
   * weights within communities.
   */
  def compressGraph(graph:Graph[VertexState,(Long,Long)],debug:Boolean=true) : Graph[VertexState,(Long,Long)] = {

    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
	// WARNING  can not use graph.mapReduceTriplets because we are mapping to new vertexIds
    val internalEdgeWeights = graph.triplets.flatMap(et=>{
    	if (et.srcAttr.community == et.dstAttr.community){
            Iterator( ( et.srcAttr.community, 2*et.attr._1) )  // count the weight from both nodes  // count the weight from both nodes
          } 
          else Iterator.empty  
    }).reduceByKey(_+_)
    
    val internalEdgeWeightsNoWeight = graph.triplets.flatMap(et=>{
    	if (et.srcAttr.community == et.dstAttr.community){
            Iterator( ( et.srcAttr.community, 2*et.attr._2) )  // count the weight from both nodes  // count the weight from both nodes
          } 
          else Iterator.empty  
    }).reduceByKey(_+_)
     
    // aggregate the internal weights of all nodes in each community
    var internalWeights = graph.vertices.values.map(vdata=> (vdata.community,vdata.internalWeight)).reduceByKey(_+_)
    var internalWeightsNoWeight = graph.vertices.values.map(vdata=> (vdata.community,vdata.internalWeightNoWeight)).reduceByKey(_+_)
    
    // join internal weights and self edges to find new interal weight of each community
    val newVerts = internalWeights.leftOuterJoin(internalWeightsNoWeight).leftOuterJoin(internalEdgeWeights)
    .leftOuterJoin(internalEdgeWeightsNoWeight)
    .map({case (vid,(((iw,iwno),iew),iewno)) =>
      val iwnoweigh=iwno.getOrElse(0L)
      val iewnoweigh=iewno.getOrElse(0L)
      val iewweigh=iew.getOrElse(0L)
      
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = iw+iewweigh
      state.nodeWeight = 0L
      state.nodeWeightNoWeight=0L
      state.internalWeightNoWeight=iwnoweigh+iewnoweigh
      (vid,state)
    }).cache()
    
    
    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et=> {
       val src = math.min(et.srcAttr.community,et.dstAttr.community)
       val dst = math.max(et.srcAttr.community,et.dstAttr.community)
       if (src != dst) Iterator(new Edge(src, dst, et.attr))
       else Iterator.empty
    }).cache()
    
    
    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
   
    val merge= (e1:(Long,Long),e2:(Long,Long)) => (e1._1+e2._1,e1._2+e2._2)
    val compressedGraph = Graph(newVerts,edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(merge)
    
    // calculate the weighted degree of each node
    val nodeWeightMapFunc = (e:EdgeTriplet[VertexState,(Long,Long)]) => Iterator((e.srcId,e.attr), (e.dstId,e.attr))
    val nodeWeightReduceFunc = (e1:(Long,Long),e2:(Long,Long)) =>(e1._1+e2._1,e1._2+e2._2)
    val nodeWeights = compressedGraph.mapReduceTriplets(nodeWeightMapFunc,nodeWeightReduceFunc)
    
    // fill in the weighted degree of each node
   // val louvainGraph = compressedGraph.joinVertices(nodeWeights)((vid,data,weight)=> { 
   val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
      val weight = weightOption.getOrElse((0L,0L))
      data.communitySigmaTot = weight._1 +data.internalWeight
      data.communitySigmaTotNoWeight=weight._2+data.internalWeightNoWeight
      data.nodeWeight = weight._1
      data.nodeWeightNoWeight=weight._2
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph
    
    newVerts.unpersist(blocking=false)
    edges.unpersist(blocking=false)
    return louvainGraph
    
   
    
  }
  
  // this funciton is used to trim the graph, to find the comminity kernel
  // the kernel's feature is that the any three vertices in the graph has a tiangle.
  def trimVertices(sc:SparkContext,graph:Graph[VertexState,(Long,Long)]):(Double,Graph[VertexState,(Long,Long)])={
    var louvainGraph = graph.cache()
    louvainGraph.edges.foreach(println)
    louvainGraph.vertices.foreach(println)
    var CurrentQ=0.0
    val msgCommunity=louvainGraph.aggregateMessages[Map[Long,Long]](sendMsgCommunity, mergeMsgCommunity)
    val temp=msgCommunity.collect()
    temp.foreach(println)
    val vertices=louvainGraph.vertices.innerJoin(msgCommunity)((vid,vdata,msgs)=>{
      val community=vdata.community
      val vstate=vdata
      val newmsg=msgs.count(x => (x._2==community))
      if (newmsg<2)
      { 
        
        vstate.community=vid
        vstate.communitySigmaTot=vstate.nodeWeight+vstate.internalWeight
        vstate.communitySigmaTotNoWeight=vstate.nodeWeightNoWeight+vstate.internalWeightNoWeight
      }
      else
      { 
        // for a triangle the the vstate.community retains otherwise,changed to the vid
        
      }
      vstate
      })
    val tmp=vertices.first()
    louvainGraph = louvainGraph.outerJoinVertices(vertices)((vid, old, newOpt) => newOpt.getOrElse(old))
    val graphWeight = louvainGraph.vertices.values.map(vdata=> vdata.internalWeight+vdata.nodeWeight).reduce(_+_)
    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVerts = louvainGraph.vertices.mapValues((vid,vdata)=> {
        // sum the nodes internal weight and all of its edges that are in its community
        val community = vdata.community
        var k_i_in = vdata.internalWeight
        var sigmaTot = vdata.communitySigmaTot.toDouble
        val M = graphWeight
        val k_i = vdata.nodeWeight + vdata.internalWeight
        var q = (k_i_in.toDouble / M) -  ( ( sigmaTot *k_i) / math.pow(M, 2) )
        //println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
        if (q < 0) 0 else q
    })  
    
   //val te=newVerts.collect()
    CurrentQ = newVerts.values.reduce(_+_)
    return (CurrentQ,louvainGraph)
  }
  
  
   /**
   * create the messages passed between each vertex to tell the same community
   */

  
  private def sendMsgCommunity( et:EdgeContext[VertexState,(Long,Long),Map[Long,Long]] ): Unit={
    et.sendToDst(Map(et.srcId->et.srcAttr.community))
    et.sendToSrc(Map(et.dstId->et.dstAttr.community)) 
   
  }
  
  private def mergeMsgCommunity(m1:Map[Long,Long],m2:Map[Long,Long]) ={
   val newMap = scala.collection.mutable.HashMap[Long,Long]()
    m1.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }
  

  
 
   // debug printing
   private def printlouvain(graph:Graph[VertexState,Long]) = {
     print("\ncommunity label snapshot\n(vid,community,sigmaTot)\n")
     graph.vertices.mapValues((vid,vdata)=> (vdata.community,vdata.communitySigmaTot)).collect().foreach(f=>println(" "+f))
   }
  
 
   
   // debug printing
   private def printedgetriplets(graph:Graph[VertexState,Long]) = {
     print("\ncommunity label snapshot FROM TRIPLETS\n(vid,community,sigmaTot)\n")
     (graph.triplets.flatMap(e=> Iterator((e.srcId,e.srcAttr.community,e.srcAttr.communitySigmaTot), (e.dstId,e.dstAttr.community,e.dstAttr.communitySigmaTot))).collect()).foreach(f=>println(" "+f))
   }
  
 
   
}