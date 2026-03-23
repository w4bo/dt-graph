package it.unibo.tests.ci.smartbench

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.utils.TimeRange
import it.unibo.graph.utils.loadTemporalRanges
import it.unibo.stats.Querying
import it.unibo.stats.runQuery
import it.unibo.tests.ci.smartbench.queries.*
import org.junit.jupiter.api.TestInstance
import org.yaml.snakeyaml.Yaml
import java.util.logging.Logger
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestSmartBenchQuery {
    val logger = Logger.getLogger(TestSmartBenchQuery::class.java.toString())
    val temporalConstraintMap = loadYamlAsMap("time_constraints.yaml")
    val machines = 1
    val testIterations = System.getenv("QUERY_ITERATIONS")?.toInt() ?: 1
    val sizes = listOf("small", "medium", "large") // , System.getenv("DATASET_SIZE")
    val querySelectivity = System.getenv("QUERY_SELECTIVITY") ?: "increased" // increased, equal

    fun loadYamlAsMap(resourcePath: String): Map<String, Any> {
        val inputStream = this::class.java.classLoader.getResourceAsStream(resourcePath) ?: throw IllegalArgumentException("$resourcePath not found in classpath")
        val yamlText = inputStream.bufferedReader().use { it.readText() }
        return Yaml().load<Map<String, Any>>(yamlText) ?: emptyMap()
    }

    private fun runTest(name: String, withRange: Boolean, queryBuilder: (graph: Graph, range: TimeRange?) -> Querying) {
        sizes.forEach { size ->
            val graph: MemoryGraphACID = MemoryGraphACID.readFromDisk("datasets/dump/smartbench/$size/") // Reload from disk
            val tsm = AsterixDBTSM.createDefault(graph, dataverse = "smartbench_$size")
            graph.tsm = tsm
            repeat(testIterations) {
                val ranges = loadTemporalRanges(temporalConstraintMap, querySelectivity, name, size)
                listOf(1, 4).forEach { threads ->
                    ranges.values.forEach { range ->
                        val query = if (withRange) {
                            queryBuilder(graph, range)
                        } else {
                            queryBuilder(graph, null)
                        }
                        logger.info("$name size: $size threads: $threads")
                        runQuery(query, "stgraph", threads, machines, "smartbench", size)
                    }
                }
            }
            graph.close()
        }
    }

    @Test
    fun runEnvironmentCoverage() = runTest("EnvironmentCoverage", false) { graph, _ ->
        EnvironmentCoverage(graph)
    }

    @Test
    fun runEnvironmentAggregate() = runTest("EnvironmentAggregate", true) { graph, range ->
        EnvironmentAggregate(graph, range!!)
    }

    @Test
    fun runMaintenanceOwners() = runTest("MaintenanceOwners", true) { graph, range ->
        MaintenanceOwners(graph, range!!)
    }

    @Test
    fun runEnvironmentOutlier() = runTest("EnvironmentOutlier", true) { graph, range ->
        EnvironmentOutlier(graph, range!!)
    }

    @Test
    fun runAgentOutlier() = runTest("AgentOutlier", true) { graph, range ->
        AgentOutlier(graph, range!!)
    }

    @Test
    fun runAgentHistory() = runTest("AgentHistory", false) { graph, _ ->
        AgentHistory(graph)
    }
}