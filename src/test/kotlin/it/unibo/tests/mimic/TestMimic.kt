package it.unibo.tests.mimic

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.*
import it.unibo.graph.utils.B
import it.unibo.graph.utils.Measurement
import it.unibo.graph.utils.Person
import it.unibo.graph.utils.VALUE
import it.unibo.stats.loadDataset
import it.unibo.tests.ci.smartbench.SmartBenchDataLoader
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import java.sql.DriverManager
import java.util.UUID
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimic {

    var graph: Graph? = null

    @BeforeAll
    fun setup() {
        graph = MemoryGraphACID.readFromDisk("datasets/dump/mimic/1000/") // Reload from disk
        val tsm = AsterixDBTSM.createDefault(graph!!, dataverse = "mimic_1000")
        graph!!.tsm = tsm
    }

    @AfterAll
    fun teardown() {
        graph!!.close()
    }

    @Test
    fun `get person`() {
        assertEquals(1, query(graph!!, listOf(Step(Person))).size)
    }

    @Test
    fun `get person with ts`() {
        assertEquals(1, query(graph!!, listOf(Step(Person), null, Step(label = B))).size)
    }

    @Test
    fun `get measurements`() {
        assertEquals(
            1,
            query(
                graph!!,
                listOf(
                    Step(Person),
                    null,
                    Step(label = B),
                    null,
                    Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GTE, 10)))
                )
            ).size
        )
    }

    @Test
    fun `get average measurements`() {
        val result = query(
            graph!!,
            listOf(Step(Person), null, Step(alias = "ts", label = B), null, Step(alias = "m", label = Measurement)),
            by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG))
        )
        assertEquals(listOf(""), result, result.toString())
    }

    @Test
    fun `filter and group by measurements`() {
        val result = query(
            graph!!,
            listOf(
                Step(Person),
                null,
                Step(alias = "ts", label = B),
                null,
                Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GTE, 10)))
            ),
            by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG))
        )
        assertEquals(listOf(""), result, result.toString())
    }

    val limit = 100L
    @Test
    fun `ingest STGraph`() {
        loadDataset(MimicIVSTGraph(limit), "stgraph",  1, 1, "mimic-iv", limit.toString())
    }

    @Test
    fun `ingest Neo4J`() {
        loadDataset(MimicIVNeo4J(limit), "neo4j",  1, 1, "mimic-iv", limit.toString())
    }

    @Test
    fun `ingest PGAge`() {
        loadDataset(MimicIVPGAge(limit), "pgage",  1, 1, "mimic-iv", limit.toString())
    }
}