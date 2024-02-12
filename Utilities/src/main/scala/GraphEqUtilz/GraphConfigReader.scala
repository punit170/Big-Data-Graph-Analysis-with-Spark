package Variables

import Variables.GraphConfigReader.config
import com.typesafe.config.ConfigFactory

object GraphConfigReader {
 val config = ConfigFactory.load()
 val randomWalkCoeff = config.getDouble("MitMStatSimVars.randomWalkCoeff")
 val numOfParallelWalks = config.getInt("MitMStatSimVars.numOfParallelWalks")
 val numItersPerCompNode = config.getInt("MitMStatSimVars.numItersPerCompNode")
 val itersBeforeAccum = config.getInt("MitMStatSimVars.itersBeforeAccum")
 val nodeMatchThreshold = config.getDouble("MitMStatSimVars.nodeMatchThreshold")
}

