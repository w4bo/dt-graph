package it.unibo.tests.smartbench

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.utils.TimeRange
import it.unibo.graph.utils.loadTemporalRanges
import it.unibo.stats.Querying
import it.unibo.stats.runQuery
import it.unibo.tests.smartbench.queries.AgentHistory
import it.unibo.tests.smartbench.queries.AgentOutlier
import it.unibo.tests.smartbench.queries.EnvironmentAggregate
import it.unibo.tests.smartbench.queries.EnvironmentCoverage
import it.unibo.tests.smartbench.queries.EnvironmentOutlier
import it.unibo.tests.smartbench.queries.MaintenanceOwners
import org.junit.jupiter.api.TestInstance
import org.yaml.snakeyaml.Yaml
import java.util.logging.Logger
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestSmartBenchQuery {
    val logger = Logger.getLogger(TestSmartBenchQuery::class.java.toString())
    val temporalConstraintMap = loadYamlAsMap("time_constraints.yaml")
    val sizes = listOf("small", "medium", "large")
    val setup: Map<String, List<String>> = mapOf(
        "localhost" to listOf("localhost"),
        "192.168.30.110" to listOf("192.168.30.110", "192.168.30.110"),
        "192.168.30.101" to listOf("192.168.30.102", "192.168.30.103"),
        "192.168.30.104" to listOf("192.168.30.105", "192.168.30.106", "192.168.30.107", "192.168.30.109")
    )

    fun loadYamlAsMap(resourcePath: String): Map<String, Any> {
        val inputStream = this::class.java.classLoader.getResourceAsStream(resourcePath) ?: throw IllegalArgumentException("$resourcePath not found in classpath")
        val yamlText = inputStream.bufferedReader().use { it.readText() }
        return Yaml().load<Map<String, Any>>(yamlText) ?: emptyMap()
    }

    private fun runTest(name: String, withRange: Boolean, queryBuilder: (graph: Graph, range: TimeRange?) -> Querying) {
        setup.forEach { (host, controllerIPs) ->
            sizes.forEach { size ->
                val graph: MemoryGraphACID = MemoryGraphACID.readFromDisk("datasets/dump/smartbench/$size/") // Reload from disk
                val tsm = AsterixDBTSM.createDefault(graph, host = host, controllerIps = controllerIPs, dataverse = "smartbench_$size")
                graph.tsm = tsm
                val querySelectivity = "increased"
                val ranges = loadTemporalRanges(temporalConstraintMap, querySelectivity, name, size)
                listOf(1, 4, 8, 16).forEach { threads ->
                    ranges.values.forEach { range ->
                        val query = if (withRange) {
                            queryBuilder(graph, range)
                        } else {
                            queryBuilder(graph, null)
                        }
                        val numMachines = if (host == "localhost") { -1 } else { controllerIPs.toSet().size }
                        logger.info("$name size: $size, threads: $threads, numMachines: $numMachines")
                        runQuery(query, "stgraph", threads, numMachines = numMachines, "smartbench", size)
                    }
                }
                graph.close()
            }
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