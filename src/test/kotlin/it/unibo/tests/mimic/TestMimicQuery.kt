package it.unibo.tests.mimic

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.*
import it.unibo.graph.utils.Measurement
import it.unibo.graph.utils.Person
import it.unibo.graph.utils.VALUE
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimicQuery {

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
        assertEquals(1, query(graph!!, listOf(Step(Person), null, Step(label = "Heart Rate"))).size)
    }

    @Test
    fun `get measurements`() {
        assertEquals(
            4,
            query(
                graph!!,
                listOf(Step(Person), null, Step("Heart Rate"), null, Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GTE, 10L))))
            ).size
        )
    }

    @Test
    fun `get average measurements`() {
        val result = query(
            graph!!,
            listOf(Step(Person), null, Step(alias = "ts", label = "Heart Rate"), null, Step(alias = "m", label = Measurement)),
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
                Step(alias = "ts", label = "Heart Rate"),
                null,
                Step(alias = "m", label = Measurement, properties = listOf(Filter(VALUE, Operators.GTE, 10L)))
            ),
            by = listOf(Aggregate("ts", "category"), Aggregate("m", VALUE, operator = AggOperator.AVG))
        )
        assertEquals(1, result.size)
    }
}