package it.unibo.tests.ci.smartbench.queries

import it.unibo.graph.interfaces.Direction
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.EdgeStep
import it.unibo.graph.query.Filter
import it.unibo.graph.query.Operators
import it.unibo.graph.query.Step
import it.unibo.graph.query.query
import it.unibo.graph.utils.HasTemperature
import it.unibo.graph.utils.Infrastructure
import it.unibo.graph.utils.Sensor
import it.unibo.graph.utils.Temperature
import it.unibo.graph.utils.hasCoverage
import it.unibo.graph.utils.hasTemperature
import it.unibo.stats.QueryResultData
import it.unibo.stats.Querying
import kotlin.system.measureTimeMillis

class EnvironmentCoverage(val graph: Graph) : Querying {
    override val queryId = "EnvironmentCoverage"
    override fun runQuery(): QueryResultData {
        val infrastructureId = "3042"
        val edgesDirectionPattern = listOf(
            Step(Infrastructure, listOf(Filter("id", Operators.EQ, infrastructureId)), alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "device"),
            Step(hasTemperature),
            Step(Temperature)
        )
        var edgesDirectionResult: List<Any>
        val edgesDirectionTime: Long = measureTimeMillis {
            edgesDirectionResult = query(
                graph,
                edgesDirectionPattern,
                by = listOf(Aggregate("Environment", "id"), Aggregate("device", "id")),
                timeaware = false
            )
        }
        return QueryResultData(edgesDirectionTime, edgesDirectionResult.size)
    }
}