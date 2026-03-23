package it.unibo.tests.ci.smartbench.queries

import it.unibo.graph.interfaces.Direction
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.*
import it.unibo.graph.utils.*
import it.unibo.stats.QueryResultData
import it.unibo.stats.Querying
import kotlin.system.measureTimeMillis

class AgentOutlier(val graph: Graph, val temporalConstraints: TimeRange) : Querying {
    override val queryId = "AgentOutlier"
    override fun runQuery(threads: Int): QueryResultData {
        /*
         * AgentOutlier: List the max value measured for each agent in each environment
         */
        val tA = temporalConstraints.from
        val tB = temporalConstraints.to
        val tau = Temperature

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            null,
            Step(HasTS),
            Step(tau, alias = "Measurement")
        )

        val edgesDirectionResult: List<Any>
        val edgesDirectionTime = measureTimeMillis {
            edgesDirectionResult = query(
                graph, edgesDirectionPattern,
                by = listOf(
                    Aggregate("Device", "id"),
                    Aggregate("Environment", "id"),
                    Aggregate("Measurement", VALUE, AggOperator.MAX)
                ),
                from = tA,
                to = tB,
                timeaware = true,
                threads = threads
            )
        }
        return QueryResultData(edgesDirectionTime, edgesDirectionResult.size)
    }
}