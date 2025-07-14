import it.unibo.evaluation.dtgraph.SmartBenchDataLoader
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Labels.*
import it.unibo.graph.query.*
import it.unibo.graph.structure.CustomGraph
import kotlin.system.measureTimeMillis

data class QueryResult(val name: String, val durationMs: Long, val count: Int)

class SmartBenchRunner(val datasetSize: String) {
    private lateinit var graph: Graph
    private val dataFiles = listOf(
        "dataset/smartbench/$datasetSize/group.json",
        /* ... altri file ... */
    )

    fun setup() {
        graph = CustomGraph(MemoryGraph())
        graph.tsm = AsterixDBTSM.createDefault(graph)
        graph.clear()
        graph.getTSM().clear()

        val loader = SmartBenchDataLoader(graph)
        val ingestionTime = measureTimeMillis {
            loader.loadData(dataFiles)
        }
        println("Loaded in ${ingestionTime/1000}s; nodes=${graph.getNodes().size}")
    }

    fun runAll(): List<QueryResult> {
        val results = mutableListOf<QueryResult>()

        // example: environmentCoverage
        var count: List<Any>; var dur: Long
        dur = measureTimeMillis {
            count = query(graph, /* pattern */)
        }
        results += QueryResult("environmentCoverage", dur, count.size)

        // aggiungi environmentAggregate, ecc.

        return results
    }
}
