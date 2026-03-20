package it.unibo.tests.ci.smartbench

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.utils.TimeRange
import it.unibo.graph.utils.loadTemporalRanges
import it.unibo.stats.Querying
import it.unibo.stats.runQuery
import it.unibo.tests.ci.smartbench.queries.AgentHistory
import it.unibo.tests.ci.smartbench.queries.AgentOutlier
import it.unibo.tests.ci.smartbench.queries.EnvironmentAggregate
import it.unibo.tests.ci.smartbench.queries.EnvironmentCoverage
import it.unibo.tests.ci.smartbench.queries.EnvironmentOutlier
import it.unibo.tests.ci.smartbench.queries.MaintenanceOwners
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.yaml.snakeyaml.Yaml
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestSmartBenchQuery {
    private val temporalConstraintMap = loadYamlAsMap("time_constraints.yaml")

    val threads = 1
    val machines = 1
    private val testIterations = System.getenv("QUERY_ITERATIONS")?.toInt() ?: 1
    private val size = System.getenv("DATASET_SIZE") ?: "small"
    private val querySelectivity = System.getenv("QUERY_SELECTIVITY") ?: "increased" // increased, equal

    fun loadYamlAsMap(resourcePath: String): Map<String, Any> {
        val inputStream = this::class.java.classLoader.getResourceAsStream(resourcePath) ?: throw IllegalArgumentException("$resourcePath not found in classpath")
        val yamlText = inputStream.bufferedReader().use { it.readText() }
        return Yaml().load<Map<String, Any>>(yamlText) ?: emptyMap()
    }

    private lateinit var graph: Graph

    @BeforeAll
    fun setup() {
        graph = MemoryGraphACID.readFromDisk("datasets/dump/smartbench/$size/") // Reload from disk
        val tsm = AsterixDBTSM.createDefault(graph, dataverse = "smartbench_$size")
        graph.tsm = tsm

        println("Loaded graph with nodes ${graph.getNodes().size}")
    }

    @AfterAll
    fun teardown() {
        graph.close()
    }

    private fun runTest(name: String, withRange: Boolean, queryBuilder: (range: TimeRange?) -> Querying) {
        repeat(testIterations) {
            val ranges = loadTemporalRanges(temporalConstraintMap, querySelectivity, name, size)
            ranges.values.forEach { range ->
                val query = if (withRange) {
                    queryBuilder(range)
                } else {
                    queryBuilder(null)
                }
                runQuery(query, "stgraph", threads, machines, "smartbench", size)
            }
        }
    }

    @Test
    fun runEnvironmentCoverage() = runTest("EnvironmentCoverage", false) {
        EnvironmentCoverage(graph)
    }

    @Test
    fun runEnvironmentAggregate() = runTest("EnvironmentAggregate", true) { range ->
        EnvironmentAggregate(graph, range!!)
    }

    @Test
    fun runMaintenanceOwners() = runTest("MaintenanceOwners", true) { range ->
        MaintenanceOwners(graph, range!!)
    }

    @Test
    fun runEnvironmentOutlier() = runTest("EnvironmentOutlier", true) { range ->
        EnvironmentOutlier(graph, range!!)
    }

    @Test
    fun runAgentOutlier() = runTest("AgentOutlier", true) { range ->
        AgentOutlier(graph, range!!)
    }

    @Test
    fun runAgentHistory() = runTest("AgentHistory", false) {
        AgentHistory(graph)
    }
}