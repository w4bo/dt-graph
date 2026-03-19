package it.unibo.tests.ci.smartbench.queries

import it.unibo.graph.interfaces.Direction
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.AggOperator
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Compare
import it.unibo.graph.query.EdgeStep
import it.unibo.graph.query.Operators
import it.unibo.graph.query.Step
import it.unibo.graph.query.query
import it.unibo.graph.utils.HasTS
import it.unibo.graph.utils.Infrastructure
import it.unibo.graph.utils.InfrastructureType
import it.unibo.graph.utils.Platform
import it.unibo.graph.utils.PlatformType
import it.unibo.graph.utils.Sensor
import it.unibo.graph.utils.Temperature
import it.unibo.graph.utils.TimeRange
import it.unibo.graph.utils.User
import it.unibo.graph.utils.VALUE
import it.unibo.graph.utils.hasCoverage
import it.unibo.graph.utils.hasOwner
import it.unibo.graph.utils.hasType_
import it.unibo.stats.QueryResultData
import it.unibo.stats.Querying
import kotlin.system.measureTimeMillis

/*
 *  EnvironmentsAggregate(𝜖, 𝜏, 𝑡𝑎 , 𝑡𝑏 ): List, for each Environment during the period [𝑡𝑎, 𝑡𝑏 [, the average value of type 𝜏 during
 * the period [𝑡𝑎, 𝑡𝑏 [ for each agent.
 */
class EnvironmentAggregate(val graph: Graph, val temporalConstraints: TimeRange) : Querying {
    override val queryId = "EnvironmentAggregate"
    override fun runQuery(): QueryResultData {
        val tA = temporalConstraints.from
        val tB = temporalConstraints.to
        val tau = Temperature

        val edgesDirectionPattern = listOf(
            listOf(
                Step(Sensor, alias = "Sensor"),
                Step(hasOwner),
                Step(User, alias = "Owner"),
                EdgeStep(hasOwner, direction = Direction.IN),
                Step(Platform, alias = "Platform"),
                Step(hasType_),
                Step(PlatformType, alias = "PlatformType"),
            ),
            listOf(
                Step(InfrastructureType),
                EdgeStep(hasType_, direction = Direction.IN),
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN),
                Step(Sensor, alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(tau, alias = "Measurement")
            ),
        )

        val edgesDirectionResult: List<Any>
        val edgesQueryTime = measureTimeMillis {
            edgesDirectionResult = query(
                graph, edgesDirectionPattern,
                where = listOf(Compare("Device", "Sensor", "id", Operators.EQ)),
                by = listOf(
                    Aggregate("PlatformType", "id"),
                    Aggregate("Owner", "id"),
                    Aggregate("Environment", "id"),
                    Aggregate("Measurement", VALUE, AggOperator.AVG)
                ),
                from = tA, to = tB, timeaware = true
            )
        }
        return QueryResultData(edgesQueryTime, edgesDirectionResult.size)
    }
}