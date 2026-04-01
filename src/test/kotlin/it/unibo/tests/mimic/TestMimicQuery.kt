package it.unibo.tests.mimic

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.*
import it.unibo.graph.utils.Measurement
import it.unibo.graph.utils.Person
import it.unibo.graph.utils.VALUE
import it.unibo.graph.utils.getTime
import it.unibo.stats.QueryResultData
import it.unibo.stats.Querying
import it.unibo.stats.runQuery
import it.unibo.tests.mimic.loaders.limitToPort
import org.junit.jupiter.api.TestInstance
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import java.sql.DriverManager
import java.util.logging.Logger
import kotlin.test.Test

private val sizes = listOf(1_692_200L, 16_922_000L, Long.MAX_VALUE)
private val setup: Map<String, List<String>> = mapOf(
    "localhost" to listOf("localhost"),
    "192.168.30.110" to listOf("192.168.30.110", "192.168.30.110"),
    "192.168.30.101" to listOf("192.168.30.102", "192.168.30.103"),
    "192.168.30.104" to listOf("192.168.30.105", "192.168.30.106", "192.168.30.107", "192.168.30.109")
)

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimicQuery {

    val logger = Logger.getLogger(TestMimicQuery::class.java.toString())
    private fun runTest(approach: String, queryBuilder: (graph: Graph) -> Querying) {
        setup.forEach { (host, controllerIPs) ->
            sizes.forEach { size ->
                val size = if (size == Long.MAX_VALUE) "full" else "$size"
                val graph: MemoryGraphACID = MemoryGraphACID.readFromDisk("datasets/dump/mimic/$size/") // Reload from disk
                val tsm = AsterixDBTSM.createDefault(graph, host = host, controllerIps = controllerIPs, dataverse = "mimic_$size")
                graph.tsm = tsm
                listOf(1, 4, 8, 16).forEach { threads ->
                    val query = queryBuilder(graph)
                    val numMachines = if (host == "localhost") { -1 } else { controllerIPs.toSet().size }
                    logger.info("${query.queryId} size: $size, threads: $threads, numMachines: $numMachines")
                    runQuery(query, approach, threads, numMachines = numMachines, "mimic-iv", size)
                }
                graph.close()
            }
        }
    }

    abstract class Q : Querying {
        override val queryId = this::class.simpleName.toString()
        override fun runQuery(threads: Int): QueryResultData {
            var res: List<Any> = listOf()
            val time = getTime {
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
                     time = getTime {
                        val result = session.run(query())
                        // Iterate over rows
                        while (result.hasNext()) {
                            val record = result.next()
                            // Get all values in the row as Any
                            val rowValues = record.keys().map { key ->
                                val value = record[key]
                                when {
                                    value.isNull -> null
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

    abstract class QPgAge(
        val graph: String,
        val url: String = "jdbc:postgresql://192.168.30.108:5435/mimic-iv",
        val user: String = "postgres",
        val pwd: String = "password"
    ) : Querying {

        override val queryId = this::class.simpleName.toString()

        override fun runQuery(threads: Int): QueryResultData {
            val res = mutableListOf<List<Any?>>()
            val time: Long
            DriverManager.getConnection(url, user, pwd).use { conn ->
                conn.createStatement().use { stmt ->
                    time = getTime {
                        val sql = query()
                        stmt.execute("LOAD 'age'")
                        stmt.execute("SET search_path = ag_catalog, public")
                        val rs = stmt.executeQuery(sql)
                        val meta = rs.metaData
                        val colCount = meta.columnCount
                        while (rs.next()) {
                            val row = ArrayList<Any?>(colCount)
                            for (i in 1..colCount) {
                                val value = rs.getObject(i)
                                row.add(value)
                            }
                            res.add(row)
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

    class Q1PGAge(graph: String) : QPgAge(graph) {
        override fun query(): String = """
            SELECT * FROM cypher('$graph'::name, $$
                MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})
                RETURN DISTINCT p, t
            $$) AS (p agtype, t agtype);
        """.trimIndent()
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
                    Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GT, 60L)))
                ),
                threads = threads
            )
        }
    }

    class Q2Neo4J(uri: String) : QNeo4J(uri) {
        override fun query(): String = "MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})-->(m:Measurement) WHERE m.value > 60 RETURN p, t, m"
    }

    class Q2PGAge(graph: String) : QPgAge(graph) {
        override fun query(): String = """
            SELECT *
            FROM (
                SELECT *
                FROM cypher('$graph'::name, $$
                    MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})
                    RETURN DISTINCT p, t, id(t)
                $$) AS (p agtype, t agtype, tsid bigint)
            ) t
            JOIN $graph.measurements m ON m.ts_id = t.tsid
            WHERE m.value > 60;
        """.trimIndent()
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

    class Q3PGAge(graph: String) : QPgAge(graph) {
        override fun query(): String = """
            SELECT t.category, avg(m.value)
            FROM (
                SELECT *
                FROM cypher('$graph'::name, $$
                    MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})
                    RETURN DISTINCT id(t), t.category
                $$) AS (tsid bigint, category text)
            ) t
            JOIN $graph.measurements m ON m.ts_id = t.tsid
            GROUP BY t.category;
        """.trimIndent()
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
                    Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GT, 60L)))
                ),
                by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG)),
                threads = threads
            )
        }
    }

    class Q4Neo4J(uri: String) : QNeo4J(uri) {
        override fun query(): String = "MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})-->(m:Measurement) WHERE m.value > 60 RETURN t.category, avg(m.value)"
    }

    class Q4PGAge(graph: String) : QPgAge(graph) {
        override fun query(): String = """
            SELECT t.category, avg(m.value)
            FROM (
                SELECT *
                FROM cypher('$graph'::name, $$
                    MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})
                    RETURN DISTINCT id(t), t.category
                $$) AS (tsid bigint, category text)
            ) t
            JOIN $graph.measurements m ON m.ts_id = t.tsid
            WHERE m.value > 60
            GROUP BY t.category;
        """.trimIndent()
    }

    @Test
    fun `filter and group by measurements`() = runTest("stgraph") { graph ->
        Q4(graph)
    }
}

fun main() {
    sizes.forEach { size ->
        listOf(
            TestMimicQuery.Q1Neo4J("bolt://localhost:${limitToPort(size)}"),
            TestMimicQuery.Q2Neo4J("bolt://localhost:${limitToPort(size)}"),
            TestMimicQuery.Q3Neo4J("bolt://localhost:${limitToPort(size)}"),
            TestMimicQuery.Q4Neo4J("bolt://localhost:${limitToPort(size)}")
        ).forEach { query ->
            runQuery(query, "neo4j", threads = 1, numMachines = -1, "mimic-iv", size.toString())
        }

        listOf(
            TestMimicQuery.Q1PGAge("mimic_$size"),
            TestMimicQuery.Q2PGAge("mimic_$size"),
            TestMimicQuery.Q3PGAge("mimic_$size"),
            TestMimicQuery.Q4PGAge("mimic_$size")
        ).forEach { query ->
            runQuery(query, "pgage", threads = 1, numMachines = -1, "mimic-iv", size.toString())
        }
    }
}