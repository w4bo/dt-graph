package it.unibo.tests.ci.smartbench.queries

import it.unibo.graph.interfaces.Direction
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Compare
import it.unibo.graph.query.EdgeStep
import it.unibo.graph.query.Filter
import it.unibo.graph.query.Operators
import it.unibo.graph.query.Step
import it.unibo.graph.query.query
import it.unibo.graph.utils.HasTS
import it.unibo.graph.utils.Infrastructure
import it.unibo.graph.utils.Sensor
import it.unibo.graph.utils.Temperature
import it.unibo.graph.utils.TimeRange
import it.unibo.graph.utils.User
import it.unibo.graph.utils.VALUE
import it.unibo.graph.utils.hasCoverage
import it.unibo.graph.utils.hasOwner
import it.unibo.graph.utils.hasTemperature
import it.unibo.stats.QueryResultData
import it.unibo.stats.Querying
import kotlin.system.measureTimeMillis

class AgentHistory(val graph: Graph) : Querying {
    override val queryId = "AgentHistory"
    override fun runQuery(): QueryResultData {
        val devices = listOf("thermometer3")
        var edgesDirectionEntitites = 0
        var edgesDirectionResult: List<Any>
        var edgesDirectionQueryTime: Long = 0

        devices.forEach {
            val edgesDirectionPattern = listOf(
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN),
                Step(Sensor, listOf(Filter("id", Operators.EQ, it)), alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(Temperature, alias = "Measurement")
            )

            edgesDirectionQueryTime += measureTimeMillis {
                edgesDirectionResult = query(
                    graph, edgesDirectionPattern,
                    by = listOf(Aggregate("Device", "id"), Aggregate("Environment", "id")),
                    timeaware = true
                )
            }
            edgesDirectionEntitites += edgesDirectionResult.size
        }

        return QueryResultData(edgesDirectionQueryTime, edgesDirectionEntitites)
    }
}