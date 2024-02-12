/*import NetGraphAlgebraDefs.{Action, NodeObject}

import java.io.{BufferedWriter, FileWriter}
import scala.io.Source

object NGStoText {
  //case class Shard(allNnodes: List[NodeObject], allN_ParentMap: immutable.Map[NodeObject, List[NodeObject]], allPnodes: List[NodeObject], allP_ParentMap: immutable.Map[NodeObject, List[NodeObject]])
  case class Graph(nodes: List[NodeObject], edges: List[Action], initNode: NodeObject)

  def stringToNodeObject(strObj: String): NodeObject = {
    //val regexPattern = """-?\d+(\.\d+[E\-\d+]?)?""".r
    val regexPattern = """-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.]+,\s?-?[\d.E-]+,\s?(?:true|false)""".r
    //    val nodeFields = regexPattern.findAllIn(strObj).toArray
    val nodeFields: Array[String] = regexPattern.findFirstIn(strObj).get.split(',')
    if (nodeFields.length != 10) {
      println(nodeFields)
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
    val valuableData = nodeFields(9).toBoolean

    NodeObject(id, children, props, currentDepth, propValueRange, maxDepth, maxBranchingFactor, maxProperties, storedValue, valuableData)
  }

  def stringToActionObject(actionstr: String): Action = {
    val nodepattern = """NodeObject\([^)]+\),""".r
    val fromToNodesArr = nodepattern.findAllIn(actionstr).toArray
    val newactionstr = nodepattern.replaceAllIn(actionstr, "")
    val actionparts = newactionstr.substring(7, newactionstr.length - 1).split(',')

    //case class Action(actionType: Int, fromNode: NodeObject, toNode: NodeObject,
    // fromId: Int, toId: Int, resultingValue: Option[Int], cost: Double)
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

  def deserializeGraph(dir: String, fileName: String): Graph = {
    val filePath = s"$dir$fileName"
    val source = Source.fromFile(filePath)
    val content = source.mkString
    val graph_string: String =content

    val tempStrArr = graph_string.split(":")

    val allGraphNodesAsString = tempStrArr(0).substring(5, tempStrArr(0).length - 1)
    val allGraphActionsAsString = tempStrArr(1).substring(5, tempStrArr(1).length - 1)

    val allGraphNodesAsStringList = """NodeObject\([^)]+\)""".r.findAllIn(allGraphNodesAsString).toList
    val allGraphActionsAsStringList = """Action\([\d]+,\s?NodeObject\([^)]+\),\s?NodeObject\([^)]+\),\s?[\d]+,\s?[\d]+,\s?(?:None|Some\([\d]+\)),\s?[0-9.]+\)""".r.findAllIn(allGraphActionsAsString).toList


    val allGraphNodes: List[NodeObject] = allGraphNodesAsStringList.map(nodeStr => stringToNodeObject(nodeStr))
    val allGraphActions: List[Action] = allGraphActionsAsStringList.map(actionStr => stringToActionObject(actionStr))
    val initNode = allGraphNodes.find(n => n.id == 0).getOrElse(throw new Exception("NodeObject with id == 0 not found in the loaded graph nodes!"))

    Graph(allGraphNodes, allGraphActions, initNode)
  }

  def serializeGraph(graph: NetGraph, dir: String, fileName: String): Unit = {
    val allNGraphNodesAsString: String = graph.sm.nodes().asScala.toList.toString()
    val allNGraphEdgesAsString: String = graph.sm.edges().asScala.toList.map(ep => (graph.sm.edgeValue(ep.source(), ep.target())).get()).toString
    val nodesEdgesDelimiter = ":"
    val oneCombinedString = allNGraphNodesAsString + nodesEdgesDelimiter + allNGraphEdgesAsString
    val writer = new BufferedWriter(new FileWriter(s"$dir$fileName"))
    writer.write(oneCombinedString)
    writer.close()
  }

//##############################################################################################

/*  Copy To NetGameSim Main.scala -> main() = {
      import NGStoText.*
      serializeGraph(g.get, outputDirectory, outGraphFileName.replace(".ngs",".txt"))
      logger.info("Saved the original graph text file!")
      serializeGraph(perturbation._1, outputDirectory, outGraphFileName.replace(".ngs", ".txt").concat(".perturbed"))
      logger.info("Saved the perturbed graph text file!")
  }*/


}*/
