package it.unibo.tests.mimic

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.*
import it.unibo.graph.utils.Measurement
import it.unibo.graph.utils.Person
import it.unibo.graph.utils.VALUE
import it.unibo.stats.QueryResultData
import it.unibo.stats.Querying
import it.unibo.stats.runQuery
import it.unibo.tests.mimic.loaders.limitToPort
import org.junit.jupiter.api.TestInstance
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import java.util.logging.Logger
import kotlin.system.measureTimeMillis
import kotlin.test.Test

val machines = 1
val testIterations = 1
val sizes = listOf(4000L) // , System.getenv("DATASET_SIZE")

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimicQuery {

    val logger = Logger.getLogger(TestMimicQuery::class.java.toString())
    private fun runTest(approach: String, queryBuilder: (graph: Graph) -> Querying) {
        sizes.forEach { size ->
            val graph: MemoryGraphACID = MemoryGraphACID.readFromDisk("datasets/dump/mimic/$size/") // Reload from disk
            val tsm = AsterixDBTSM.createDefault(graph, dataverse = "mimic_$size")
            graph.tsm = tsm
            repeat(testIterations) {
                listOf(1, 4, 8).forEach { threads ->
                    val query = queryBuilder(graph)
                    logger.info("${query.queryId} size: $size threads: $threads")
                    runQuery(query, approach, threads, machines, "mimic", size.toString())
                }
            }
            graph.close()
        }
    }

    abstract class Q : Querying {
        override val queryId = this::class.simpleName.toString()
        override fun runQuery(threads: Int): QueryResultData {
            val res: List<Any>
            val time = measureTimeMillis {
                res = query(threads)
            }
            return QueryResultData(time, res.size)
        }

        abstract fun query(threads: Int): List<Any>
    }

    abstract class QNeo4J(val uri: String = "bolt://localhost:7687", val user: String = "neo4j", val pwd: String = "password") : Querying {
        override val queryId = this::class.simpleName.toString()
        override fun runQuery(threads: Int): QueryResultData {
            val res = mutableListOf<List<Any?>>()
            val time: Long
            GraphDatabase.driver(uri, AuthTokens.basic(user, pwd)).use { driver ->
                driver.session().use { session ->
                     time = measureTimeMillis {
                        val result = session.run(query())
                        // Iterate over rows
                        while (result.hasNext()) {
                            val record = result.next()
                            // Get all values in the row as Any
                            val rowValues = record.keys().map { key ->
                                val value = record[key]
                                // Convert Neo4j Value to raw object
                                when {
                                    value.isNull -> null
                                    // value.isBoolean -> value.asBoolean()
                                    // value.isNumber -> value.asObject()
                                    // value.isString -> value.asString()
                                    else -> value.asObject()
                                }
                            }
                            res.add(rowValues)
                        }
                    }
                }
            }
            return QueryResultData(time, res.size)
        }

        abstract fun query(): String
    }

    class Q1(val graph: Graph) : Q() {
        override fun query(threads: Int): List<Any> {
            return query(graph, listOf(Step(Person), null, Step(label = "HR")), threads = threads)
        }
    }

    class Q1Neo4J(uri: String) : QNeo4J(uri) {
        override fun query(): String = "MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'}) RETURN p, t"
    }

    @Test
    fun `get person with ts`() = runTest("stgraph") { graph ->
        Q1(graph)
    }

    class Q2(val graph: Graph) : Q() {
        override fun query(threads: Int): List<Any> {
            return query(
                graph,
                listOf(
                    Step(Person),
                    null,
                    Step("HR"),
                    null,
                    Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GTE, 10L)))
                ),
                threads = threads
            )
        }
    }

    class Q2Neo4J(uri: String) : QNeo4J(uri) {
        override fun query(): String = "MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})-->(m:Measurement) WHERE m.value > 10 RETURN p, t, m"
    }

    @Test
    fun `get measurements`() = runTest("stgraph") { graph ->
        Q2(graph)
    }

    class Q3(val graph: Graph) : Q() {
        override fun query(threads: Int): List<Any> {
            return query(
                graph,
                listOf(
                    Step(Person),
                    null,
                    Step(alias = "ts", label = "HR"),
                    null,
                    Step(alias = "m", label = Measurement)
                ),
                by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG)),
                threads = threads
            )
        }
    }

    class Q3Neo4J(uri: String) : QNeo4J(uri) {
        override fun query(): String = "MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})-->(m:Measurement) RETURN t.category, avg(m.value)"
    }

    @Test
    fun `get average measurements`() = runTest("stgraph") { graph ->
        Q3(graph)
    }

    class Q4(val graph: Graph) : Q() {
        override fun query(threads: Int): List<Any> {
            return query(
                graph,
                listOf(
                    Step(Person),
                    null,
                    Step(alias = "ts", label = "HR"),
                    null,
                    Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GTE, 10L)))
                ),
                by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG)),
                threads = threads
            )
        }
    }

    class Q4Neo4J(uri: String) : QNeo4J(uri) {
        override fun query(): String = "MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})-->(m:Measurement) WHERE m.value > 10 RETURN t.category, avg(m.value)"
    }

    @Test
    fun `filter and group by measurements`() = runTest("stgraph") { graph ->
        Q4(graph)
    }
}

fun main() {
    repeat(testIterations) {
        sizes.forEach { size ->
            listOf(TestMimicQuery.Q1Neo4J("bolt://localhost:${limitToPort(size)}"),
                TestMimicQuery.Q2Neo4J("bolt://localhost:${limitToPort(size)}"),
                TestMimicQuery.Q3Neo4J("bolt://localhost:${limitToPort(size)}"),
                TestMimicQuery.Q4Neo4J("bolt://localhost:${limitToPort(size)}")).forEach { query ->
                runQuery(query, "neo4j", 1, machines, "mimic", size.toString())
            }
        }
    }
}