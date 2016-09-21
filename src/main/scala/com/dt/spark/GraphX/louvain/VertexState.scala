package com.dt.spark.GraphX.louvain

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable{

  var community = -1L
  var communitySigmaTot = 0L
  var internalWeight = 0L  // self edges
  var nodeWeight = 0L;  //out degree
  var changed = false
  // 2016-9-20 add, add new feature to without the weight of the initial edge
  var internalWeightNoWeight=0L
  var nodeWeightNoWeight=0L
  var communitySigmaTotNoWeight=0L
  
  //////////////////////////////////////////////////////////////////////////////
   
  override def toString(): String = {
    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
    ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+",nodeWeightNoWeight:"+nodeWeightNoWeight+
    ",internalWeightNoWeight:"+internalWeightNoWeight+",communitySigmaTotNoWeight:"+communitySigmaTotNoWeight +"}"
  }
}