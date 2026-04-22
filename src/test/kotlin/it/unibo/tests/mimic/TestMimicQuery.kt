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
import it.unibo.stats.TestConfig
import it.unibo.stats.runQuery
import it.unibo.tests.mimic.queries.MimicBy
import it.unibo.tests.mimic.queries.MimicFilter
import org.junit.jupiter.api.TestInstance
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import java.io.File
import java.sql.DriverManager
import kotlin.test.Test


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimicQuery {

    abstract class Q : Querying {
        override val queryId = this::class.simpleName.toString()
        override fun runQuery(threads: Int, queryMode: QueryMode): QueryResultData {
            var res: List<Any> = listOf()
            val time = getTime {
                res = query(threads, queryMode)
            }
            return QueryResultData(time, res.size)
        }

        abstract fun query(threads: Int, queryMode: QueryMode): List<Any>
    }

    abstract class QNeo4J(val uri: String = "bolt://localhost:7687", val user: String = "neo4j", val pwd: String = "password") : Querying {
        override val queryId = this::class.simpleName.toString()
        override fun runQuery(threads: Int, queryMode: QueryMode): QueryResultData {
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

        override fun runQuery(threads: Int, queryMode: QueryMode): QueryResultData {
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
        override fun query(threads: Int, queryMode: QueryMode): List<Any> {
            return query(graph, listOf(Step(Person), null, Step(label = "HR")), threads = threads, mode = queryMode)
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
    fun `get person with ts`() = TestConfig.runTest(dataset = "mimic") { graph, size, mode ->
        Q1(graph)
    }

    class Q2(val graph: Graph) : Q() {
        override fun query(threads: Int, queryMode: QueryMode): List<Any> {
            return query(
                graph,
                listOf(
                    Step(Person),
                    null,
                    Step("HR"),
                    null,
                    Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GT, 140L)))
                ),
                threads = threads,
                mode = queryMode
            )
        }
    }

    class Q2Neo4J(uri: String) : QNeo4J(uri) {
        override fun query(): String = "MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})-->(m:Measurement) WHERE m.value > 140 RETURN p, t, m"
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
            WHERE m.value > 140;
        """.trimIndent()
    }

    @Test
    fun `get measurements`() = TestConfig.runTest(dataset = "mimic") { graph, size, mode ->
        Q2(graph)
    }

    class Q3(val graph: Graph) : Q() {
        override fun query(threads: Int, queryMode: QueryMode): List<Any> {
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
                threads = threads,
                mode = queryMode
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
    fun `get average measurements`() = TestConfig.runTest(dataset = "mimic") { graph, size, mode ->
        Q3(graph)
    }

    class Q4(val graph: Graph) : Q() {
        override fun query(threads: Int, queryMode: QueryMode): List<Any> {
            return query(
                graph,
                listOf(
                    Step(Person),
                    null,
                    Step(alias = "ts", label = "HR"),
                    null,
                    Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GT, 140L)))
                ),
                by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG)),
                threads = threads,
                mode = queryMode
            )
        }
    }

    class Q4Neo4J(uri: String) : QNeo4J(uri) {
        override fun query(): String = "MATCH (p:Person)-->(t:TimeSeries {abbreviation: 'HR'})-->(m:Measurement) WHERE m.value > 140 RETURN t.category, avg(m.value)"
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
            WHERE m.value > 140
            GROUP BY t.category;
        """.trimIndent()
    }

    @Test
    fun `filter and group by measurements`() = TestConfig.runTest(dataset = "mimic") { graph, size, mode ->
        Q4(graph)
    }

    @Test
    fun testMode() {
        val dataset = "mimic"
        val size = "full" // "1692200"
        val path = "datasets/dump/$dataset/$size/"
        val graph = MemoryGraphACID.readFromDisk(path)
        val tsm = AsterixDBTSM.createDefault(
            graph,
            host = "192.168.30.110",
            controllerIps = listOf("192.168.30.110"),
            dataverse = "${dataset}_$size"
        )
        graph.tsm = tsm
        File("src/main/resources/mimic-iv_subjectids_tsids.csv").useLines { lines ->
            lines
                .toList()
                .map { it.trim().split(",") }
                .map { Triple(it[0].toLong(), it[1].toLong(), it[2].toLong()) }
                .filter { (subject_id, itemid, c) -> itemid == 220277L } // HR
                .filterIndexed { index, _ -> index % 10 == 0 }
                .take(100)
                .forEach { (subject_id, itemid, c) ->
                    listOf(1, 2).forEach {
                        QueryMode.entries.forEach { mode ->
                            runQuery(MimicBy(graph, subject_id, c), "stgraph", threads = 1, numMachines = 1, dataset, size = size, mode = mode)
                            runQuery(MimicFilter(graph, subject_id, c), "stgraph", threads = 1, numMachines = 1, dataset, size = size, mode = mode)
                        }
                    }
                }
        }
        graph.close()
    }
}

fun main() {
//    mimic_sizes.forEach { size ->
//        listOf(
//            TestMimicQuery.Q1Neo4J("bolt://localhost:${limitToPort(size)}"),
//            TestMimicQuery.Q2Neo4J("bolt://localhost:${limitToPort(size)}"),
//            TestMimicQuery.Q3Neo4J("bolt://localhost:${limitToPort(size)}"),
//            TestMimicQuery.Q4Neo4J("bolt://localhost:${limitToPort(size)}")
//        ).forEach { query ->
//            runQuery(query, "neo4j", threads = 1, numMachines = -1, "mimic", size.toString(), mode = QueryMode.NAIVE)
//        }
//
//        listOf(
//            TestMimicQuery.Q1PGAge("mimic_$size"),
//            TestMimicQuery.Q2PGAge("mimic_$size"),
//            TestMimicQuery.Q3PGAge("mimic_$size"),
//            TestMimicQuery.Q4PGAge("mimic_$size")
//        ).forEach { query ->
//            runQuery(query, "pgage", threads = 1, numMachines = -1, "mimic", size.toString(), mode = QueryMode.NAIVE)
//        }
//    }
}