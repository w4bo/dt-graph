package it.unibo.graph

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.GraphMemory
import it.unibo.graph.structure.CustomGraph

object App {
    //val tsm = MemoryTSManager()
    val g: CustomGraph = CustomGraph(GraphMemory())
    val tsm = AsterixDBTSM.createDefault()
    // val tsm = RocksDBTSM()
    // val g: CustomGraph = CustomGraph(GraphRocksDB())
}