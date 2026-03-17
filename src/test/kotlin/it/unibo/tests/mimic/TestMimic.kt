package it.unibo.tests.mimic

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.*
import it.unibo.graph.utils.Measurement
import it.unibo.graph.utils.Person
import it.unibo.graph.utils.VALUE
import it.unibo.stats.Loader
import it.unibo.stats.loadDataset
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimic {

    var graph: Graph? = null

    @BeforeAll
    fun setup() {
        graph = MemoryGraphACID.readFromDisk("datasets/dump/mimic/100/") // Reload from disk
        val tsm = AsterixDBTSM.createDefault(graph!!, dataverse = "mimic_100")
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
        assertEquals(1, query(graph!!, listOf(Step(Person), null, Step(label = "Temperature F"))).size)
    }

    @Test
    fun `get measurements`() {
        assertEquals(
            4,
            query(
                graph!!,
                listOf(Step(Person), null, Step("Temperature F"), null, Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GTE, 10L))))
            ).size
        )
    }

    @Test
    fun `get average measurements`() {
        val result = query(
            graph!!,
            listOf(Step(Person), null, Step(alias = "ts", label = "Temperature F"), null, Step(alias = "m", label = Measurement)),
            by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG))
        )
        assertEquals(1, result.size)
    }

    @Test
    fun `filter and group by measurements`() {
        val result = query(
            graph!!,
            listOf(
                Step(Person),
                null,
                Step(alias = "ts", label = "Temperature F"),
                null,
                Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GTE, 10L)))
            ),
            by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG))
        )
        assertEquals(1, result.size)
    }

    val limit = 100L
    val threads = 1
    val machines = 1

    fun load(loader: Loader, model: String, dataset: String) {
        loadDataset(loader, model, threads, machines, dataset, limit.toString())
    }

    @Test
    fun `ingest STGraph`() {
        load(MimicIVSTGraph(limit), "stgraph", "mimic-iv")
    }

    @Test
    fun `ingest Neo4J`() {
        load(MimicIVNeo4J(limit), "neo4j", "mimic-iv")
    }

    @Test
    fun `ingest PGAge`() {
        load(MimicIVPGAge(limit), "pgage", "mimic-iv")
    }
}