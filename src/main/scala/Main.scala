package mitm

import Variables.GraphConfigReader._
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.Logger
import HelperFunction._
import LoggingUtil.GraphUtil.CreateLogger
import NetGraphAlgebraDefs.NodeObject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}


object Main {

  val logger: Logger = CreateLogger(Main.getClass)
  val props: Config = ConfigFactory.load()

  class MapAccumulator extends AccumulatorV2[immutable.Map[NodeObject, (NodeObject, Float)], immutable.Map[NodeObject, (NodeObject, Float)]] {
    type mapType = mutable.Map[NodeObject, (NodeObject, Float)]
    type IN = immutable.Map[NodeObject, (NodeObject, Float)]
    type OUT = immutable.Map[NodeObject, (NodeObject, Float)]

    private val mapAcc: mapType = mutable.Map.empty[NodeObject, (NodeObject, Float)]

    override def add(v: IN): Unit = {
      v.foreach { case (pgNode, (ngNode, srval)) =>
        if (!mapAcc.contains(pgNode))
          mapAcc += pgNode -> (ngNode, srval)
        else if (mapAcc(pgNode)._2 < srval)
          mapAcc(pgNode) = (ngNode, srval)
        else {}
      }
    }
    override def reset(): Unit = {mapAcc.clear()}
    override def copy(): AccumulatorV2[IN, OUT] = {new MapAccumulator()}
    override def isZero: Boolean = mapAcc.isEmpty
    override def value: OUT = mapAcc.toMap
    override def merge(other: AccumulatorV2[IN, OUT]): Unit = {mapAcc ++= other.value}
  }

  def main(args: Array[String]): Unit = {
    logger.info("Program started!")

    //checks
    assert(itersBeforeAccum<=numItersPerCompNode)

    //loading program env variable
    assert(args(0) == "local" || args(0) == "hdfs" || args(0) == "aws")

    //get environment properties
    val envProps = props.getConfig(args(0))

    //load graphs
    val originalNetGraph: NetGraph = loadGraph(dir = envProps.getString("NGSGraphDir"), fileName = envProps.getString("originalGraphFileName"), masterURL = envProps.getString("masterURL"))

    val perturbedNetGraph: NetGraph = loadGraph(dir = envProps.getString("NGSGraphDir"), fileName = envProps.getString("perturbedGraphFileName"), masterURL = envProps.getString("masterURL"))

    //spark config
    val conf = new SparkConf().setAppName("YoSpark").setMaster(envProps.getString("master"))
    val sc = new SparkContext(conf)

    //accumulator hash table for all partitions; also stores final simrank results
    val mapAccumulator = new MapAccumulator()
    sc.register(mapAccumulator, "DriverResultAccumulator")


    val startNodes = getStartNodes(perturbedNetGraph)
    val perturbedGraph_BV = sc.broadcast(perturbedNetGraph)
    val originalGraph_BV = sc.broadcast(originalNetGraph)

    val startNodesRDD: RDD[(Long, NodeObject)] = createRDDForRW(sc, startNodes, numOfParallelWalks)

   //Main logic
   //-> random walks run on separate partitions
   //-> simranks are performed
   //-> results are send to accumulator and shared among other partitions for optimization
    val mapReduceResults = startNodesRDD.mapPartitions { partition =>
      partition.map { case (key, startNode) =>
        val quotient = numItersPerCompNode / itersBeforeAccum
        val remainder = numItersPerCompNode % itersBeforeAccum
        val itersBeforeAccumList = {
          if (remainder > 0)
            (1 to quotient).map(_ => itersBeforeAccum).toList ::: List(remainder)
          else
            (1 to quotient).map(_ => itersBeforeAccum).toList
        }

        val visitedNodesList = ListBuffer.empty[NodeObject]
        val randomWalkSubGraphs = ListBuffer.empty[NetGraph]
        val simRankMaps = ListBuffer.empty[immutable.Map[NodeObject, List[(NodeObject, Float)]]]

        for (iter <- itersBeforeAccumList) {
          for (j <- 1 to iter) {
            val randomWalkResult = simpleRandomWalk(perturbedGraph_BV.value, startNode, visitedNodesList.toList)
            print(s"Randomwalk> partition:$key -- i:$iter -- j:$j  "); println(randomWalkResult.nodes.map(n => n.id))
            randomWalkSubGraphs += randomWalkResult
            visitedNodesList ++= randomWalkResult.nodes
          }

          for (j <- 1 to randomWalkSubGraphs.length) {
            val simRankResult = SimRankv_2(randomWalkSubGraphs(j - 1).nodes, generateParentMap(randomWalkSubGraphs(j - 1)), originalGraph_BV.value.nodes, generateParentMap(originalGraph_BV.value), mapAccumulator)
            if (simRankResult.nonEmpty)
              simRankMaps += simRankResult
          }

          println("Simrank maps inside")
          println(simRankMaps.map(m => m.map(ele => (ele._1.id, ele._2.map(tpl => (tpl._1.id, tpl._2))))))
          val reducedSimRankResult = {
            if (simRankMaps.nonEmpty) {
              val nonEmptyMaps = simRankMaps.map(m => m.filter(_._2.nonEmpty)).filter(m => m.nonEmpty)
              if(nonEmptyMaps.nonEmpty && nonEmptyMaps.length > 1){
                nonEmptyMaps.reduce((map1, map2) =>
                  (map1.toSeq ++ map2.toSeq)
                    .groupBy(_._1)
                    .map { case (key, tpls) =>
                      key -> tpls.map(_._2).reduce((ls1, ls2) => ls1 ++ ls2)
                    }
                ).map { case (key, ls) =>
                  key -> findBestNodeMatch(key, ls)
                }.filter { case (_, (ngNode, _)) =>
                  ngNode.valuableData
                }
              }
              else{}
            }
            else{}
          }

          reducedSimRankResult match {
            case r: immutable.Map[NodeObject, (NodeObject, Float)] =>
              mapAccumulator.add(r)
            case _ =>
          }

          println(s"Accumulator state after $iter iterations of partition $key:")
          println(mapAccumulator.value.map(ele => (ele._1.id, (ele._2._1.id, ele._2._2))))
        }

        (key, visitedNodesList.distinct, randomWalkSubGraphs.toList.distinct, simRankMaps.toList.distinct)
      }
    }.collect().toList

    //unpersist after algorithm finishes
    originalGraph_BV.unpersist()
    perturbedGraph_BV.unpersist()

    //log all important results
    println("All partitionWise visited Nodes:")
    mapReduceResults.foreach(ele => println(ele._1, ele._2.map(n => n.id)))
    println("All partitionWise Distinct random walks:")
    mapReduceResults.foreach(ele => {println(s"par:${ele._1}"); ele._3.map(g => println(s"\t${g.nodes.map(n=>n.id)}"))})
    println("All partitionWise SimRank Calculations:")
    mapReduceResults.foreach(ele => {
    println(s"par:${ele._1}"); ele._4.map(g => println(s"\t${g.map{ case (key, value) => (key.id, value.map{case (node, srval) => (node.id, srval)})}}"))
    })

    val SimRankAccumResultIdMap = mapAccumulator.value.map(ele => (ele._1.id, (ele._2._1.id, ele._2._2)))
    println(s"SimRankResults from all partitions: $SimRankAccumResultIdMap")
    val mapaccnodeids = mapAccumulator.value.map(ele => ele._1.id).toList

    val origvaldatanodeids = originalNetGraph.nodes.filter(_.valuableData == true).map(_.id)
    println(s"Original Graph Valuable Nodes: $origvaldatanodeids")
    val leftoutnodeids = origvaldatanodeids.diff(mapaccnodeids)
    println(s"Nodes not covered by random walks: $leftoutnodeids")

    //original Graph perturbations
//    val originalGraphNodeIds = originalNetGraph.nodes.map(_.id)
//    val perturbedGraphNodeIds = perturbedNetGraph.nodes.map(_.id)

//    val removedNodesIds = originalGraphNodeIds.diff(perturbedGraphNodeIds)
//    val addedNodesIds = perturbedGraphNodeIds.diff(originalGraphNodeIds)
//    val modifiedNodesIds = originalNetGraph.nodes.flatMap(n=> perturbedNetGraph.nodes.find(p => {p.id == n.id} && n!=p)).map(_.id)
//    val unperturbedIds = originalGraphNodeIds.diff(removedNodesIds).diff(modifiedNodesIds)

    //other Stats
    val correctMatches = SimRankAccumResultIdMap.toList.filter { case (pid, (nid, srval)) => (pid == nid) && (srval > nodeMatchThreshold)}.map{case (pid, (nid, _)) => (pid, nid)}
    val numOfCorrectMatches = correctMatches.length
    val incorrectMatches = SimRankAccumResultIdMap.toList.filter { case (pid, (nid, srval)) => (pid != nid) && (srval > nodeMatchThreshold) }.map{case (pid, (nid, _)) => (pid, nid)}
    val numOfIncorrectMatches = incorrectMatches.length

    val RandomWalksPerPartitionList: List[List[List[Int]]] = mapReduceResults.map(_._3.map(_.nodes.map(_.id)))
    println("RandomWalksPerPartitionList")
    println(RandomWalksPerPartitionList)

    val unsuccessWalk: List[Int] => Boolean = (w: List[Int]) => w.intersect(incorrectMatches.map(_._1)).nonEmpty
    val successWalk: List[Int] => Boolean = (w: List[Int]) => w.intersect(correctMatches.map(_._1)).nonEmpty && w.intersect(incorrectMatches.map(_._1)).isEmpty

    val numOfUnsuccWalksPerPart: List[Int] = RandomWalksPerPartitionList.map(rws => rws.count(w => unsuccessWalk(w)))
    println("number of distinct unsuccessful Walks per part:")
    println(numOfUnsuccWalksPerPart)
    println("total unsuccessful walks:")
    println(numOfUnsuccWalksPerPart.sum)

    val numOfSuccWalksPerPart: List[Int] = RandomWalksPerPartitionList.map(rws => rws.count(w => successWalk(w)))
    println("number of distinct successful Walks per part:")
    println(numOfSuccWalksPerPart)
    println("Total successful walks:")
    println(numOfSuccWalksPerPart.sum)

    val allMitMStatistics = Map("Original valuable data nodes:" -> origvaldatanodeids.toString,
                                "Perturbed nodes not covered by random walks:" -> leftoutnodeids.toString,
                                "Correct matches (True Positive):" -> numOfCorrectMatches.toString,
                                "Incorrect matches (False positive):" -> numOfIncorrectMatches.toString,
                                "Number of distinct unsuccessful walks per part:" -> numOfUnsuccWalksPerPart.toString,
                                "Number of total distinct unsuccessful walks:" -> numOfUnsuccWalksPerPart.sum.toString,
                                "Number of distinct successful walks per part:" -> numOfSuccWalksPerPart.toString,
                                "Number of total distinct successful walks:" -> numOfSuccWalksPerPart.sum.toString
                                )

    writeYamlFile(data=allMitMStatistics, dir=envProps.getString("outputDir"), fileName=envProps.getString("statisticsOutputFileName"),masterURL=envProps.getString("masterURL"))
    logger.info("Statistics File Written successfully!")
    println("Program Ends!")
  }

}