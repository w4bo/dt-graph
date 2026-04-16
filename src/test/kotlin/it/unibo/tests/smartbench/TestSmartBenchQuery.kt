package it.unibo.tests.smartbench

import it.unibo.graph.query.QueryMode
import it.unibo.stats.TestConfig
import it.unibo.tests.smartbench.queries.*
import org.junit.jupiter.api.TestInstance
import kotlin.test.Test


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestSmartBenchQuery {
    @Test
    fun runEnvironmentCoverage() =
        TestConfig.runTest(dataset = "smartbench") { graph, size, _ ->
            if (size != "extralarge") EnvironmentCoverage(graph) else null
        }

    @Test
    fun runEnvironmentAggregate() = TestConfig.runTest(dataset = "smartbench") { graph, size, mode ->
        if (size != "extralarge") EnvironmentAggregate(graph, size) else null
    }

    @Test
    fun runMaintenanceOwners() = TestConfig.runTest(dataset = "smartbench") { graph, size, mode ->
        if (size != "extralarge") MaintenanceOwners(graph, size) else null
    }

    @Test
    fun runEnvironmentOutlier() = TestConfig.runTest(dataset = "smartbench") { graph, size, mode ->
        if (size != "extralarge") EnvironmentOutlier(graph, size) else null
    }

    @Test
    fun runAgentOutlier() = TestConfig.runTest(dataset = "smartbench") { graph, size, mode ->
        if (size == "extralarge" || (size == "large" && mode == QueryMode.NAIVE)) null else AgentOutlier(graph, size)
    }

    @Test
    fun runAgentHistory() = TestConfig.runTest(dataset = "smartbench") { graph, size, mode ->
        if (size != "extralarge") AgentHistory(graph) else null
    }
}