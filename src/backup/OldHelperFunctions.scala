/*
import Main.logger
import NetGraphAlgebraDefs.{Action, NetGraphComponent, NodeObject}
import Variables.GraphConfigReader._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import java.io.{FileInputStream, ObjectInputStream}
import java.net.URI
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.util.Random

object OldHelperFunctions {

  case class NetGraph(nodes: List[NodeObject], edges: List[Action], initNode: NodeObject)

  type VD = NodeObject
  type ED = Action

  def load_graph(fileName: String, dir: String, FS: String): NetGraph = {
    if (FS == "local" || FS == "file:///") {
      val fileInputStream: FileInputStream = new FileInputStream(s"$dir$fileName")
      val objectInputStream: ObjectInputStream = new ObjectInputStream(fileInputStream)
      val ng = objectInputStream.readObject.asInstanceOf[List[NetGraphComponent]]
      logger.info("Test")
      val nodesAndEdges: (List[NodeObject], List[Action]) = (
        ng.collect { case node: NodeObject => node }.toList,
        ng.collect { case edge: Action => edge }.toList
      )
      val initNode = nodesAndEdges._1.find(n => n.id == 0).getOrElse(throw new Exception("NodeObject with id == 0 not found in the loaded graph nodes!"))
      objectInputStream.close()
      fileInputStream.close()

      val nodes = nodesAndEdges._1

      //Action(actionType: Int, fromNode: NodeObject, toNode: NodeObject, fromId: Int, toId: Int, resultingValue: Option[Int], cost: Double)
      val edges = nodesAndEdges._2.map{ac =>
        val newFromNode = nodes.find(_.id == ac.fromNode.id).get
        val newToNode = nodes.find(_.id == ac.toNode.id).get
        Action(ac.actionType, newFromNode, newToNode, ac.fromId, ac.toId, ac.resultingValue, ac.cost)
      }

      NetGraph(nodes, edges, initNode)
    }

    else if (FS.startsWith("hdfs://") || FS.startsWith("s3://")) {
      val conf = new org.apache.hadoop.conf.Configuration()
      val fs = FileSystem.get(URI.create(hadoopFS), conf)

      val hdfsFilePath = new Path(s"$dir$fileName")

      val objectInputStream: ObjectInputStream = fs.open(hdfsFilePath) match {
        case inputStream =>
          new ObjectInputStream(inputStream)
      }

      val ng = objectInputStream.readObject.asInstanceOf[List[NetGraphComponent]]
      objectInputStream.close()

      val nodesAndEdges: (List[NodeObject], List[Action]) = (
        ng.collect { case node: NodeObject => node }.toList,
        ng.collect { case edge: Action => edge }.toList
      )
      val initNode = nodesAndEdges._1.find(n => n.id == 0).getOrElse(throw new Exception("NodeObject with id == 0 not found in the loaded graph nodes!"))
      NetGraph(nodes=nodesAndEdges._1, edges=nodesAndEdges._2, initNode)
    }
    else
      throw new Exception("hadoopFS should be set to either on local path, hdfs localhost path or s3 bucket path")
  }

  def createNetGraphX(sc: SparkContext, graph: NetGraph): Graph[VD, ED] = {
    //vertices
    val vertexTuples: List[(VertexId, VD)] = graph.nodes.map(node => (node.id.toLong, node))
    val edges: List[Edge[ED]] = graph.edges.map(edge => Edge(edge.fromNode.id.toLong, edge.toNode.id.toLong, edge))

    val verticesRDD: RDD[(VertexId, VD)] = sc.parallelize(vertexTuples)
    val edgesRDD: RDD[Edge[ED]] = sc.parallelize(edges)


    println("verticesRDD:")
    verticesRDD.foreach(ele => println(ele._1))
    println("edgesRDD:")
    edgesRDD.foreach(action => println(s"(${action.srcId}, ${action.dstId})"))

    Graph(verticesRDD, edgesRDD)
  }

  def getStartNodes(graphx: Graph[VD, ED]): List[VD] = {
    val vertexIds = graphx.vertices.keys.collect().toList
    println(s"all vertexIds:$vertexIds")
    val vertexIdsWithInEdges = graphx.collectNeighbors(EdgeDirection.In).filter(ele => ele._2.nonEmpty).keys.collect().toList
    println(s"vertexIdsWithInEdges:$vertexIdsWithInEdges")

    println(s"DIFFERENCE:${vertexIds.diff(vertexIdsWithInEdges)}")

    val startNodes = vertexIds.diff(vertexIdsWithInEdges).map(vId => graphx.vertices.lookup(vId).head)
    println("START NODES:")
    startNodes.foreach(sn => println(sn))
    startNodes
  }

  def getStartNodes(netGraph: NetGraph): List[NodeObject] = {
    val allNodes = netGraph.nodes.distinct
    val inEdgesNodes = netGraph.edges.map(ac => ac.toNode).distinct
    val startNodes = allNodes.diff(inEdgesNodes)
    startNodes
  }

  def createSubGraph(graph: Graph[VD, ED], subNodes: List[VD]): (Graph[VD, ED], NetGraph) = {
    val subNodeIds: List[VertexId] = subNodes.map(n => n.id.toLong)

    // Define your filtering criteria
    val vertexFilter: (VertexId, VD) => Boolean = (vertexId, vertexData) => {
      subNodeIds.contains(vertexId)
    }

    val edgeFilter: EdgeTriplet[VD, ED] => Boolean = (edgeTriplet) => {
      subNodeIds.contains(edgeTriplet.srcId) || subNodeIds.contains(edgeTriplet.dstId)
    }

    // Create the induced subgraph based on the filtering criteria
    val subgraph = graph.subgraph(vpred=vertexFilter, epred=edgeFilter)

    val netGraphNodes: List[NodeObject] = subNodes
    val netGraphEdges: List[Action] = subgraph.edges.map(edgeED => edgeED.attr).collect().toList
    val initNode: NodeObject = getStartNodes(graph).head

    val subNetGraph = NetGraph(netGraphNodes, netGraphEdges, initNode)
    (subgraph, subNetGraph)
  }

  def randomWalkForGraphX(initNodes: List[VD], graphx: Graph[VD, ED]): (List[VD], Graph[VD, ED], NetGraph)  = {

    val visitedNodes = ListBuffer.empty[VD]
    val vertices = graphx.vertices
    var currentVertex = initNodes(Random.nextInt(initNodes.length))
    var currentVertexID: VertexId = currentVertex.id.toLong
    val childMap = graphx.collectNeighbors(EdgeDirection.Out)
    /*println("\ngraphx vertices:")
    graphx.vertices.foreach(v => print(s"${v._1}, "))
    println("\ngraphx num vertices:")
    println(graphx.numVertices)
    println("\nmy graphx num vertices:")
    println(graphx.vertices.count())
    println(s"visitedNodes.size.toLong: ${visitedNodes.size.toLong}")
    println(s"randomWalkCoeff * graphx.numVertices: ${randomWalkCoeff * graphx.numVertices}")
    */
    while (visitedNodes.size < randomWalkCoeff * graphx.numVertices) {

//      println("current vertex: ")
//      println(currentVertex)
      // Add the current node to the list of visited nodes

      visitedNodes += currentVertex

//      println(s"visitedNodes: $visitedNodes")

      // Retrieve the neighbors of the current node
      val neighbors = childMap.lookup(currentVertexID).flatMap(ele => ele).map(ele => ele._2).toArray

//      println("neighbors:")
//      neighbors.foreach(ele => println(ele))

      if (neighbors.isEmpty) {
        // If there are no neighbors, terminate the random walk
//        println("No neighbors")
        val subNodes = visitedNodes.toList
        val subgraphTuple = createSubGraph(graphx, subNodes)
        return (subNodes, subgraphTuple._1, subgraphTuple._2)
      }
      else {
        // Choose a random neighbor to continue the random walk
        val randomNextVertexArrayIndex: Int = Random.nextInt(neighbors.length)
//        println("next chosen vertex: ")
//        println(randomNextVertexArrayIndex)

        currentVertex = neighbors(randomNextVertexArrayIndex)
        currentVertexID = currentVertex.id.toLong
      }
    }

    val subNodes = visitedNodes.toList
    val subgraphTuple = createSubGraph(graphx, subNodes)
    (subNodes, subgraphTuple._1, subgraphTuple._2)
  }

  def generateParentMap(graph: NetGraph): immutable.Map[NodeObject, List[NodeObject]] = {
//    val edgeList: List[(NodeObject, NodeObject)] = graph.edges.map(ac => (nodes.find(_.id == ac.fromNode.id).get, nodes.find(_.id == ac.toNode.id).get))
    val edgeList: List[(NodeObject, NodeObject)] = graph.edges.map(ac => (ac.fromNode, ac.toNode))

    val tempNGraphAdjacencyMap = mutable.Map[NodeObject, ListBuffer[NodeObject]]()
    edgeList.foreach { ep =>
      tempNGraphAdjacencyMap.getOrElseUpdate(ep._2, mutable.ListBuffer.empty) += ep._1
    }
    val parentMap: immutable.Map[NodeObject, List[NodeObject]] = tempNGraphAdjacencyMap.foldLeft(immutable.Map[NodeObject, List[NodeObject]]())((acc, m) => acc + (m._1 -> m._2.toList))
    parentMap

  }


/*  def initializeSRMap(PgNodes: List[NodeObject], NgNodes: List[NodeObject]): mutable.Map[(NodeObject, NodeObject), Float] = {
    val SRMap = mutable.Map[(NodeObject, NodeObject), Float]()
    PgNodes.foreach(pNode =>
      NgNodes.foreach(nNode => {
        if (pNode == nNode) {
          SRMap += (pNode, nNode) -> 1.0f
        }
        else {
          SRMap += (pNode, nNode) -> 0.0f
        }
      }
      )
    )
    SRMap
  }*/

  def SimRankv_2(PgNodes: List[NodeObject], PgParentMap: immutable.Map[NodeObject, List[NodeObject]], NgNodes: List[NodeObject], NgParentMap: immutable.Map[NodeObject, List[NodeObject]]): immutable.Map[NodeObject, List[(NodeObject, Float)]] = {
    val SRMap = mutable.Map[(NodeObject, NodeObject), Float]()

    PgNodes.foreach(pNode =>
      NgNodes.foreach(nNode => {
        if (pNode == nNode)  {
          SRMap += (pNode, nNode) -> 1.0f
        }
        else {
          SRMap += (pNode, nNode) -> 0.0f
        }
      }
     )
    )
    PgNodes.foreach(pNode =>
      NgNodes.foreach(nNode =>

        if (pNode != nNode) {
          val pParentNodes = PgParentMap.get(pNode)
          val nParentNodes = NgParentMap.get(nNode)

          pParentNodes match {
            case Some(pParentList) =>
              nParentNodes match {
                case Some(nParentList) =>
                  if((pNode.id == 16 || pNode.id == 17) && (nNode.id == 1 || nNode.id == 2))
                    println("got here!")
                  val coeffPart: Float = 1.0f / (pParentList.length * nParentList.length)

                  val combos = ListBuffer[(NodeObject, NodeObject)]()
                  pParentList.foreach(pParentNode => nParentList.foreach(nParentNode => combos += ((pParentNode, nParentNode))))

                  val sumPart = combos.foldLeft(0.0f) { case (acc, (pParentNode, nParentNode)) =>
                    acc + SRMap(pParentNode, nParentNode)
                  }
                  //assert(coeffPart * sumPart != 0)
                  SRMap((pNode, nNode)) = BigDecimal(coeffPart * sumPart).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
                case None =>
                  SRMap += (pNode, nNode) -> 0.0f
              }
            case None =>
              SRMap += (pNode, nNode) -> 0.0f
          }
        }
      )
    )

    val SROut: immutable.Map[NodeObject, List[(NodeObject, Float)]] = SRMap.groupBy { case ((node1, _), _) => node1 }.map {
      case (node, entries) =>
        val entryList = entries.map { case ((_, node2), value) => (node2, value) }.toList.filterNot(entry => entry._2 == 0)
        (node, entryList)
    }
    SROut
  }

  def findBestNodeMatch(PGNode: NodeObject, NGNodeMatches: List[(NodeObject, Float)]): (NodeObject, Float) = {
    for (matchpair <- NGNodeMatches) {
      if (PGNode == matchpair._1) {
        return matchpair
      }
    }

    val scores = NGNodeMatches.foldLeft(mutable.Map[NodeObject, Int]())((acc, ele) => acc + (ele._1 -> 0))

    NGNodeMatches.foreach { ngnodematch =>
      if (PGNode.children == ngnodematch._1.children) scores(ngnodematch._1) += 1
      if (PGNode.props == ngnodematch._1.props) scores(ngnodematch._1) += 1
      if (PGNode.maxDepth == ngnodematch._1.maxDepth) scores(ngnodematch._1) += 1
      if (PGNode.maxProperties == ngnodematch._1.maxProperties) scores(ngnodematch._1) += 1
    }
    println(scores)
    NGNodeMatches.find(ele => ele._1 == scores.toList.sortBy(-_._2).head._1) match
      {
        case Some(matchpair)
        => matchpair
        case None => throw new Exception("best score element not found in Perturbed SubGraph Nodes!")
      }
  }

  def randomWalkForRDDNetGraph(rddNodes: RDD[NodeObject], rddEdges: RDD[Action], initNode: NodeObject): List[NodeObject] = {

    val visitedNodes = ListBuffer.empty[NodeObject]
    var currentVertex = initNode
    val nodes = rddNodes.collect().toList

    val edgeList: RDD[(NodeObject, NodeObject)] = {
      rddEdges.map(ac => (ac.fromNode, ac.toNode))
    }

    def getChildNodes(parentNode: NodeObject, edgeList: RDD[(NodeObject, NodeObject)]): RDD[NodeObject] = {
      edgeList.filter(tpl => tpl._1 == parentNode).map(tpl => tpl._2)
    }

    while (visitedNodes.size < randomWalkCoeff * nodes.length) {

      println("current vertex: ")
      println(currentVertex)

      visitedNodes += currentVertex

      println(s"visitedNodes: $visitedNodes")

      val neighbors = getChildNodes(currentVertex, edgeList)

      println("neighbors:")
      neighbors.foreach(ele => println(ele))

      if (neighbors.isEmpty) {
        println("No neighbors")
        return visitedNodes.toList
      }
      else {
        //        val randomNextVertexArrayIndex: Int = Random.nextInt(neighbors.count().toInt)
        //        println("next chosen vertex: ")
        //        println(randomNextVertexArrayIndex)

        currentVertex = neighbors.takeSample(false, 1)(0)
      }
    }
    return visitedNodes.toList
  }

/*    def simpleRandomWalk(Nodes: List[NodeObject], Edges: List[Action], initNode: NodeObject): List[NodeObject] = {

      val visitedNodes = ListBuffer.empty[NodeObject]
      var currentVertex = initNode

      val edgeList: List[(NodeObject, NodeObject)] = {
        Edges.map(ac => (ac.fromNode, ac.toNode))
      }

      def getChildNodes(parentNode: NodeObject, edgeList: List[(NodeObject, NodeObject)]): List[NodeObject] = {
        edgeList.filter(tpl => tpl._1 == parentNode).map(tpl => tpl._2)
      }

      while (visitedNodes.size < randomWalkCoeff * Nodes.length) {

//        println("current vertex: ")
//        println(currentVertex)

        visitedNodes += currentVertex

//        println(s"visitedNodes: $visitedNodes")

        val neighbors = getChildNodes(currentVertex, edgeList)

//        println("neighbors:")
//        neighbors.foreach(ele => println(ele))

        if (neighbors.isEmpty) {
//          println("No neighbors")
          return visitedNodes.toList
        }
        else {
          val randomNextVertexArrayIndex: Int = Random.nextInt(neighbors.length)
//          println("next chosen vertex: ")
//          println(randomNextVertexArrayIndex)

          currentVertex = neighbors(randomNextVertexArrayIndex)
        }
      }

    visitedNodes.toList
  }*/

  def simpleRandomWalk(netGraph: NetGraph, initNode: NodeObject): NetGraph = {

    val Nodes = netGraph.nodes
    val Edges = netGraph.edges
    val visitedNodes = ListBuffer.empty[NodeObject]
    var currentVertex = initNode

    val edgeList: List[(NodeObject, NodeObject)] = {
      Edges.map(ac => (ac.fromNode, ac.toNode))
    }

    def getChildNodes(parentNode: NodeObject, edgeList: List[(NodeObject, NodeObject)]): List[NodeObject] = {
      edgeList.filter(tpl => tpl._1 == parentNode).map(tpl => tpl._2)
    }

    while (visitedNodes.size < randomWalkCoeff * Nodes.length) {

      //        println("current vertex: ")
      //        println(currentVertex)

      visitedNodes += currentVertex

      //        println(s"visitedNodes: $visitedNodes")

      val neighbors = getChildNodes(currentVertex, edgeList)

      //        println("neighbors:")
      //        neighbors.foreach(ele => println(ele))

      if (neighbors.isEmpty) {
        //          println("No neighbors")
        val subGraphNodes = visitedNodes.toList.distinct
        val subGraphEdges = Edges.filter(ac => subGraphNodes.contains(ac.fromNode) && subGraphNodes.contains(ac.toNode))
        val subGraphInitNode = subGraphNodes.diff(subGraphEdges.map(ac => ac.toNode).distinct).head
        NetGraph(subGraphNodes, subGraphEdges, subGraphInitNode)
      }
      else {
        val randomNextVertexArrayIndex: Int = Random.nextInt(neighbors.length)
        //          println("next chosen vertex: ")
        //          println(randomNextVertexArrayIndex)

        currentVertex = neighbors(randomNextVertexArrayIndex)
      }
    }

    val subGraphNodes = visitedNodes.toList.distinct
    val subGraphEdges = Edges.filter(ac => subGraphNodes.contains(ac.fromNode) && subGraphNodes.contains(ac.toNode))
    val subGraphInitNode = subGraphNodes.diff(subGraphEdges.map(ac => ac.toNode).distinct).head
    NetGraph(subGraphNodes, subGraphEdges, subGraphInitNode)
  }

  def createRDDForRW(sc: SparkContext, startNodes: List[NodeObject], numOfParallelWalks: Int): RDD[(Long, NodeObject)] = {
    val graphTupleList = (1L to numOfParallelWalks.toLong).foldLeft(List.empty[(Long, NodeObject)])((acc, num) => acc ::: List((num, startNodes(Random.nextInt(startNodes.length)))))
    val rddTuples: RDD[(Long, NodeObject)] = sc.parallelize(graphTupleList)
    rddTuples
  }

  def createPerturbedSubgraphRDD(sc: SparkContext, randomWalkSubGraphs: List[NetGraph], numOfParallelWalks: Int): RDD[(Long, NetGraph)] = {

    val graphTupleList = (1 to numOfParallelWalks).foldLeft(List.empty[(Long, NetGraph)])((acc, num) => acc ::: List((num.toLong, randomWalkSubGraphs(num-1))))
    val rddTuples: RDD[(Long, NetGraph)] = sc.parallelize(graphTupleList)
    rddTuples
  }


}
*/
