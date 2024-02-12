package mitm

import Main.{MapAccumulator, logger}
import NetGraphAlgebraDefs.{Action, NetGraphComponent, NodeObject}
import Variables.GraphConfigReader._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import java.io.{BufferedWriter, FileInputStream, FileWriter, ObjectInputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.io.Source

object HelperFunction {

  //new NetGraph class
  case class NetGraph(nodes: List[NodeObject], edges: List[Action], initNode: NodeObject)

  //function to convert a string to NodeObject
  def stringToNodeObject(strObj: String): NodeObject = {
    val regexPattern = """-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.E-]+,\s?(?:true|false)""".r
    val nodeFields: Array[String] = regexPattern.findFirstIn(strObj).get.split(',')
    if (nodeFields.length != 10) {
      println(nodeFields.mkString("Array(", ", ", ")"))
      logger.info(s"NodeStr: $strObj doesn't have 10 fields!")
      throw new Exception(s"NodeStr: $strObj doesn't have 10 fields!")

    }
    /*NodeObject(id: Int, children: Int, props: Int, currentDepth: Int = 1, propValueRange:Int,
     maxDepth:Int, maxBranchingFactor:Int, maxProperties:Int, storedValue: Double)*/

    val id = nodeFields(0).toInt
    val children = nodeFields(1).toInt
    val props = nodeFields(2).toInt
    val currentDepth = nodeFields(3).toInt
    val propValueRange = nodeFields(4).toInt
    val maxDepth = nodeFields(5).toInt
    val maxBranchingFactor = nodeFields(6).toInt
    val maxProperties = nodeFields(7).toInt
    val storedValue = nodeFields(8).toDouble
    val valuableData = nodeFields(9).strip().toBoolean

    NodeObject(id, children, props, currentDepth, propValueRange, maxDepth, maxBranchingFactor, maxProperties, storedValue, valuableData)
  }

  //function to convert a string to Action
  def stringToActionObject(actionstr: String): Action = {
    val nodepattern = """NodeObject\([^)]+\),""".r
    val fromToNodesArr = nodepattern.findAllIn(actionstr).toArray
    val newactionstr = nodepattern.replaceAllIn(actionstr, "")
    val actionparts = newactionstr.substring(7, newactionstr.length - 1).split(',')

    /*case class Action(actionType: Int, fromNode: NodeObject, toNode: NodeObject,
      fromId: Int, toId: Int, resultingValue: Option[Int], cost: Double)*/
    val actionType = actionparts(0).toInt
    val fromNode = stringToNodeObject(fromToNodesArr(0))
    val toNode = stringToNodeObject(fromToNodesArr(1))
    val fromId = actionparts(1).toInt
    val toId = actionparts(2).toInt
    val resultingValue: Option[Int] = {
      if (actionparts(3) == "None") None
      else if (actionparts(3).startsWith("Some")) Some(actionparts(3).substring(5, actionparts(3).length - 1).toInt)
      else None
    }
    val cost = actionparts(4).toDouble
    val action = Action(actionType, fromNode, toNode, fromId, toId, resultingValue, cost)
    action
  }

  //function to load NetGraph from a text file
  def loadGraph(dir: String, fileName: String, masterURL: String): NetGraph = {
    val filePath = s"$dir$fileName"
    if (masterURL.startsWith("hdfs://") || masterURL.startsWith("s3://")) {
      val conf = new org.apache.hadoop.conf.Configuration()
      val fs = FileSystem.get(new URI(masterURL), conf)
      val hdfsFilePath = new Path(filePath)

      if (!fs.exists(hdfsFilePath)) {
        logger.info(s"File does not exist at $filePath.")
        throw new Exception(s"File does not exist at $filePath.")
      }
      else {
        val source = Source.fromInputStream(fs.open(hdfsFilePath))
        val content = source.mkString
        source.close()
        deserializeGraph(content)
      }
    }
    else if(masterURL == "local" || masterURL == "file:///"){
      val source = Source.fromFile(filePath)
      val content = source.mkString
      source.close()
//      println(content)
      deserializeGraph(content)
    }
    else {
      logger.info("hadoopFS should be set to either on local path, hdfs localhost path or s3 bucket path")
      throw new Exception("hadoopFS should be set to either on local path, hdfs localhost path or s3 bucket path")
    }
  }

  //deserialize NetGraph from string read from graph's text file
  def deserializeGraph(graph_string: String): NetGraph = {

    val tempStrArr = graph_string.split(":")

    val allGraphNodesAsString = tempStrArr(0).substring(5, tempStrArr(0).length - 1)
    val allGraphActionsAsString = tempStrArr(1).substring(5, tempStrArr(1).length - 1)

    val allGraphNodesAsStringList = """NodeObject\([^)]+\)""".r.findAllIn(allGraphNodesAsString).toList
    val allGraphActionsAsStringList = """Action\([\d]+,\s?NodeObject\([^)]+\),\s?NodeObject\([^)]+\),\s?[\d]+,\s?[\d]+,\s?(?:None|Some\([\d]+\)),\s?[0-9.]+\)""".r.findAllIn(allGraphActionsAsString).toList


    val allGraphNodes: List[NodeObject] = allGraphNodesAsStringList.map(nodeStr => stringToNodeObject(nodeStr))
    val allGraphActions: List[Action] = allGraphActionsAsStringList.map(actionStr => stringToActionObject(actionStr))
    val initNode = allGraphNodes.find(n => n.id == 0).getOrElse(throw new Exception("NodeObject with id == 0 not found in the loaded graph nodes!"))

    NetGraph(allGraphNodes, allGraphActions, initNode)
  }

  //function to load NetGraph from graph's ngs file
  def loadGraphFromNGS(dir: String, fileName: String, masterURL: String): NetGraph = {

    if ((masterURL == "local") || (masterURL == "file:///")) {
      val fileInputStream: FileInputStream = new FileInputStream(s"$dir$fileName")
      val objectInputStream: ObjectInputStream = new ObjectInputStream(fileInputStream)
      val ng = objectInputStream.readObject.asInstanceOf[List[NetGraphComponent]]
      logger.info("Test")
      val nodesAndEdges: (List[NodeObject], List[Action]) = (
        ng.collect { case node: NodeObject => node },
        ng.collect { case edge: Action => edge }
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

    else if (masterURL.startsWith("hdfs://") || masterURL.startsWith("s3://")) {
      val conf = new org.apache.hadoop.conf.Configuration()
      val fs = FileSystem.get(URI.create(masterURL), conf)

      val hdfsFilePath = new Path(s"$dir$fileName")

      val objectInputStream: ObjectInputStream = fs.open(hdfsFilePath) match {
        case inputStream =>
          new ObjectInputStream(inputStream)
      }

      val ng = objectInputStream.readObject.asInstanceOf[List[NetGraphComponent]]
      objectInputStream.close()

      val nodesAndEdges: (List[NodeObject], List[Action]) = (
        ng.collect { case node: NodeObject => node },
        ng.collect { case edge: Action => edge }
      )
      val initNode = nodesAndEdges._1.find(n => n.id == 0).getOrElse(throw new Exception("NodeObject with id == 0 not found in the loaded graph nodes!"))
      NetGraph(nodes=nodesAndEdges._1, edges=nodesAndEdges._2, initNode)
    }
    else
      logger.info("masterURL should be set to either on local path, hdfs localhost path or s3 bucket path")
      throw new Exception("masterURL should be set to either on local path, hdfs localhost path or s3 bucket path")
  }

  //function to find nodes in graphs that have no incoming edges and can serve as good starting points
  def getStartNodes(netGraph: NetGraph): List[NodeObject] = {
    val allNodes = netGraph.nodes.distinct
    val inEdgesNodes = netGraph.edges.map(ac => ac.toNode).distinct
    val startNodes = allNodes.diff(inEdgesNodes)
    startNodes
  }

  //function to get a child->List[Parents] map for a NetGraph
  def generateParentMap(graph: NetGraph): immutable.Map[NodeObject, List[NodeObject]] = {
    val edgeList: List[(NodeObject, NodeObject)] = graph.edges.map(ac => (ac.fromNode, ac.toNode))

    val tempNGraphAdjacencyMap = mutable.Map[NodeObject, ListBuffer[NodeObject]]()
    edgeList.foreach { ep =>
      tempNGraphAdjacencyMap.getOrElseUpdate(ep._2, mutable.ListBuffer.empty) += ep._1
    }
    val parentMap: immutable.Map[NodeObject, List[NodeObject]] = tempNGraphAdjacencyMap.foldLeft(immutable.Map[NodeObject, List[NodeObject]]())((acc, m) => acc + (m._1 -> m._2.toList))
    parentMap

  }

  //SIMRANK
  def SimRankv_2(allPgNodes: List[NodeObject], PgParentMap: immutable.Map[NodeObject, List[NodeObject]], allNgNodes: List[NodeObject], NgParentMap: immutable.Map[NodeObject, List[NodeObject]], mapAcc: MapAccumulator): immutable.Map[NodeObject, List[(NodeObject, Float)]] = {
    val ignorePgNodes = mapAcc.value.keys.toList
    val ignoreNgNodes = mapAcc.value.map(ele => ele._2._1).toList

/*    println("mapACC inside SIMRANK:")
    println(mapAcc.value.map(ele => (ele._1.id, (ele._2._1.id, ele._2._2))))*/

    val PgNodes = allPgNodes.diff(ignorePgNodes)
    val NgNodes = allNgNodes.diff(ignoreNgNodes)
    val SRMap = mutable.Map[(NodeObject, NodeObject), Float]()

/*    println("PG Nodes inside SIMRANK:")
    PgNodes.foreach(n => print(s"${n.id}, "))

    println("\nNG Nodes inside SIMRANK:")
    NgNodes.foreach(n => print(s"${n.id}, "))*/

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
      NgNodes.foreach(nNode => {
        if (pNode != nNode) {
          val pParentNodes = PgParentMap.get(pNode)
          val nParentNodes = NgParentMap.get(nNode)

          pParentNodes match {
            case Some(pParentList) =>
              nParentNodes match {
                case Some(nParentList) =>
                  val coeffPart: Float = 1.0f / (pParentList.length * nParentList.length)

                  val combos = ListBuffer[(NodeObject, NodeObject)]()
                  pParentList.foreach(pParentNode => nParentList.foreach(nParentNode => combos += ((pParentNode, nParentNode))))

                  val sumPart = combos.foldLeft(0.0f) { case (acc, (pParentNode, nParentNode)) =>
                    acc + SRMap.getOrElse((pParentNode, nParentNode), mapAcc.value.map { case (pgNode, (ngNode, srval)) =>
                      ((pgNode, ngNode), srval)}.getOrElse((pParentNode, nParentNode), 0.0f))
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
      }
      )
    )

    val SROut: immutable.Map[NodeObject, List[(NodeObject, Float)]] = SRMap.groupBy { case ((node1, _), _) => node1 }.map {
      case (node, entries) =>
        val entryList = entries.map { case ((_, node2), value) => (node2, value) }.toList.filterNot(entry => entry._2 == 0)
        (node, entryList)
    }

/*    println("\nSIMRANK Output:")
    println(SROut.map{ case (key, value) => (key.id, value.map{case (node, srval) => (node.id, srval)})})*/
    SROut
  }

  //find best match among nodes that
  def findBestNodeMatch(PGNode: NodeObject, NGNodeMatches: List[(NodeObject, Float)]): (NodeObject, Float) = {

    if(NGNodeMatches.length == 1){
      return NGNodeMatches.head
    }

    for (matchpair <- NGNodeMatches) {
      if (PGNode == matchpair._1) {
        return matchpair
      }
    }

    val scores = mutable.Map.empty[NodeObject, Int]
    NGNodeMatches.foreach(ele => scores += (ele._1 -> 0))

    NGNodeMatches.foreach { ngnodematch =>
      if (PGNode.children == ngnodematch._1.children) scores(ngnodematch._1) += 1
      if (PGNode.props == ngnodematch._1.props) scores(ngnodematch._1) += 1
      if (PGNode.maxDepth == ngnodematch._1.maxDepth) scores(ngnodematch._1) += 1
      if (PGNode.maxProperties == ngnodematch._1.maxProperties) scores(ngnodematch._1) += 1
    }
    println(scores)
    NGNodeMatches.find(ele => ele._1 == scores.toList.sortBy(-_._2).head._1) match
      {
        case Some(matchpair) => matchpair
        case None => throw new Exception("best score element not found in Original Graph Nodes!")
      }
  }

  // function to perform random walk
  // walks size depends on "randomWalkCoeff" parameter set in application.conf
  def simpleRandomWalk(netGraph: NetGraph, initNode: NodeObject, visitedNodesList: List[NodeObject]): NetGraph = {

    val Nodes = netGraph.nodes
    val Edges = netGraph.edges
    val visitedNodes = ListBuffer.empty[NodeObject]
    val currentVertex = ListBuffer(initNode)

    val edgeList: List[(NodeObject, NodeObject)] = {
      Edges.map(ac => (ac.fromNode, ac.toNode))
    }

    def getChildNodes(parentNode: NodeObject, edgeList: List[(NodeObject, NodeObject)]): List[NodeObject] = {
      edgeList.filter(tpl => tpl._1 == parentNode).map(tpl => tpl._2)
    }

    while (visitedNodes.size < randomWalkCoeff * Nodes.length) {

      //        println("current vertex: ")
      //        println(currentVertex)
      visitedNodes += currentVertex.head

      //        println(s"visitedNodes: $visitedNodes")

      val neighbors = getChildNodes(currentVertex.head, edgeList)

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
        val unvisitedNodes = neighbors.diff(visitedNodesList)
        //          println("next chosen vertex: ")
        //          println(randomNextVertexArrayIndex)
        currentVertex.clear()
        currentVertex += {
          if (unvisitedNodes.nonEmpty)
            unvisitedNodes(Random.nextInt(unvisitedNodes.length))
          else
            neighbors(Random.nextInt(neighbors.length))
        }
      }
    }

    val subGraphNodes = visitedNodes.toList.distinct
    val subGraphEdges = Edges.filter(ac => subGraphNodes.contains(ac.fromNode) && subGraphNodes.contains(ac.toNode))
    val subGraphInitNode = subGraphNodes.diff(subGraphEdges.map(ac => ac.toNode).distinct).head
    NetGraph(subGraphNodes, subGraphEdges, subGraphInitNode)
  }

  //function to create an RDD to parallelize startnodes
  def createRDDForRW(sc: SparkContext, startNodes: List[NodeObject], numOfParallelWalks: Int): RDD[(Long, NodeObject)] = {
    val selectedNodes = ListBuffer.empty[NodeObject]
    val graphTupleList = (1L to numOfParallelWalks.toLong).foldLeft(List.empty[(Long, NodeObject)])((acc, num) => acc ::: List((num, {
      val unvisitedNodes = startNodes.diff(selectedNodes.toList)
      if (unvisitedNodes.nonEmpty)
        unvisitedNodes(Random.nextInt(unvisitedNodes.length))
      else
        startNodes(Random.nextInt(startNodes.length))
    })))
    val rddTuples: RDD[(Long, NodeObject)] = sc.parallelize(graphTupleList)
    rddTuples
  }

  //function to write the final statistics yaml file
  def writeYamlFile(data: immutable.Map[String, Any], dir: String, fileName: String, masterURL: String): Unit = {

    // Function to convert Map to ListMap
    def convertToOrderedMap(input: immutable.Map[String, Any]): java.util.Map[String, Any] = {
      val result = new java.util.LinkedHashMap[String, Any]()
      input.foreach {
        case (key, value) => result.put(key, value)
      }
      result
    }

    val filePath = dir.concat(fileName)
    val options = new DumperOptions()
    options.setPrettyFlow(true)
    val yaml = new Yaml(options)
    val fileWriter =
      if (masterURL == "local" || masterURL == "file:///") {
        new FileWriter(filePath)
      } else if (masterURL.startsWith("hdfs://") || masterURL.startsWith("s3://")) {
        val conf = new org.apache.hadoop.conf.Configuration()
        val fs = FileSystem.get(URI.create(masterURL), conf)
        val hdfsPath = new Path(filePath)
        val outputStream = fs.create(hdfsPath)
        new BufferedWriter(new java.io.OutputStreamWriter(outputStream, StandardCharsets.UTF_8))
      } else {
        throw new IllegalArgumentException("Unsupported HadoopFS parameter")
      }

    try {
      yaml.dump(convertToOrderedMap(data), fileWriter)
      println(s"Data written to $filePath")
    } finally {
      fileWriter.close()
    }
  }


  //for future use
/*  def createPerturbedSubgraphRDD(sc: SparkContext, randomWalkSubGraphs: List[(Long, List[NetGraph])], numOfParallelWalks: Int): RDD[(Long, List[NetGraph])] = {
//    val graphTupleList = (1 to numOfParallelWalks).foldLeft(List.empty[(Long, List[NetGraph])])((acc, num) => acc ::: List((num.toLong, randomWalkSubGraphs(num-1))))
    val rddTuples: RDD[(Long, List[NetGraph])] = sc.parallelize(randomWalkSubGraphs)
    rddTuples
  }

    def createValuableDataSubGraph(netGraph: NetGraph): NetGraph = {
    val subGraphNodes = netGraph.nodes.filter(n => n.valuableData==true)
    val subGraphEdges = netGraph.edges.filter(ac => subGraphNodes.contains(ac.fromNode) || subGraphNodes.contains(ac.toNode))
    val subGraphInitNode = subGraphNodes.diff(subGraphEdges.map(ac => ac.toNode).distinct).head
    NetGraph(subGraphNodes, subGraphEdges, subGraphInitNode)
  }*/


}
