import NetGraphAlgebraDefs.{Action, NodeObject}
import mitm.HelperFunction._
import mitm.Main._
import Variables.GraphConfigReader._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class MitMStatSimTest extends AnyFlatSpec with Matchers {
  behavior of "Tests!"

  it should "throw error while reading a graph text file that does not exists" in {
    assertThrows[Exception](loadGraph(dir = "dummy/path", fileName = "Graph1000.ngs.txt", masterURL = "local"))
  }

  it should "throw error while reading a graph ngs file that does not exists" in {
    assertThrows[Exception](loadGraphFromNGS(dir = "dummy/path", fileName = "Graph1000.ngs", masterURL = "local"))
  }

  it should "throw an error if NodeObject has less than 10 fields" in {
    val node: String = "NodeObject(1,2,3,4,5,6,7,8,9)" //missing valuable data field
    assertThrows[Exception](stringToNodeObject(node))
  }

  it should "convert a string successfully to a NodeObject even with negative numbers" in {
    val node: String = "NodeObject(1,2,3,4,5,6,7,8,-9.0,true)"
    assert(stringToNodeObject(node) == NodeObject(1,2,3,4,5,6,7,8,-9.0,true))
  }

  it should "convert a string successfully to an Action" in {
    val actionString = "Action(1,NodeObject(2,3,4,5,6,7,8,9,10.0,true),NodeObject(3,4,5,6,7,8,9,10,11.0,false),2,3,Some(4),5.0)"
    val edge1 = Action(1,NodeObject(2,3,4,5,6,7,8,9,10.0,true),NodeObject(3,4,5,6,7,8,9,10,11.0,false),2,3,Some(4),5.0)
    assert(stringToActionObject(actionString) == edge1)
  }

  it should "random walks to be run in every partition before accumulating results should be set <= total number of random walks to be run per partition)" in {
    assert(itersBeforeAccum<=numItersPerCompNode) //set parameters in application.conf
  }

}

