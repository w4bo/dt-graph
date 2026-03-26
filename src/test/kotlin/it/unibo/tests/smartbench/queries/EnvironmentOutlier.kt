package it.unibo.tests.smartbench.queries

import it.unibo.graph.interfaces.Direction
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.*
import it.unibo.graph.utils.*
import it.unibo.stats.QueryResultData
import it.unibo.stats.Querying
import kotlin.system.measureTimeMillis

class EnvironmentOutlier(val graph: Graph, val temporalConstraints: TimeRange) : Querying {
    override val queryId = "EnvironmentOutlier"
    override fun runQuery(threads: Int): QueryResultData {/*
           * EnvironmentAlert: List the environments that have had a an average temperature > 20 degrees during the period [𝑡𝑎, 𝑡𝑏 [.
           */
        val tA = temporalConstraints.from
        val tB = temporalConstraints.to
        val minTemp = 50.5
        var edgesDirectionResult: List<Any>
        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            null,
            Step(HasTS),
            Step(Temperature, alias = "Measurement")
        )
        val edgesDirectionQueryTime = measureTimeMillis {
            edgesDirectionResult = query(
                graph,
                edgesDirectionPattern,
                where = listOf(Compare("Environment", "Measurement", LOCATION, Operators.ST_INTERSECTS)),
                by = listOf(Aggregate("Environment", "id"), Aggregate("Measurement", VALUE, AggOperator.AVG)),
                from = tA,
                to = tB,
                timeaware = true,
                threads = threads
            )
        }
        edgesDirectionResult = edgesDirectionResult.filter { ((it as ArrayList<*>)[1]!! as Double) > minTemp }
        return QueryResultData(edgesDirectionQueryTime, edgesDirectionResult.size)
    }
}