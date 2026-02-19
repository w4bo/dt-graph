package it.unibo.tests.socialnetwork

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Labels
import it.unibo.graph.interfaces.Labels.AgriParcel
import it.unibo.graph.interfaces.Labels.Device
import it.unibo.graph.interfaces.Labels.HasTS
import it.unibo.graph.interfaces.Labels.Measurement
import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.P
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.interfaces.TS
import it.unibo.graph.query.AggOperator
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Step
import it.unibo.graph.query.query
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.ID
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.VALUE
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertTrue
import java.sql.DriverManager
import kotlin.test.assertEquals

class TestSocialNetwork {
    companion object {
        val snb = SnbCsvImportTest()
        var g: Graph? = null

        @BeforeAll
        @JvmStatic
        fun load() {
            snb.load()
            g = setup(snb.postgres.jdbcUrl)
        }
        @JvmStatic
        @AfterAll fun stop() {
            snb.stop()
        }
        const val cLimit = 1000
        @JvmStatic
        fun setup(host: String, limit: Int? = cLimit): Graph {
            val g = MemoryGraphACID()
            val tsm = AsterixDBTSM.createDefault(g)
            g.tsm = tsm
            g.clear()
            tsm.clear()
            DriverManager.getConnection(host, "test", "test").use { conn ->
                // --------------------
                // Load PERSON
                // --------------------
                conn.createStatement().use { stmt ->
                    val rs = stmt.executeQuery("SELECT * FROM person")

                    while (rs.next()) {
                        val n = g.addNode(Labels.Person, isTs = true)
                        g.addProperty(n.id, "id", rs.getLong("id"), PropType.LONG)
                        g.addProperty(n.id, "firstName", rs.getString("firstName"), PropType.STRING)
                        g.addProperty(n.id, "lastName", rs.getString("lastName"), PropType.STRING)
                        g.addProperty(n.id, "gender", rs.getString("gender"), PropType.STRING)
                        g.addProperty(n.id, "birthday", rs.getLong("birthday"), PropType.LONG)
                        g.addProperty(n.id, "creationDate", rs.getLong("creationDate"), PropType.LONG)
                        g.addProperty(n.id, "locationIP", rs.getString("locationIP"), PropType.STRING)
                        g.addProperty(n.id, "browserUsed", rs.getString("browserUsed"), PropType.STRING)
                    }
                }
                // --------------------
                // Load COMMENT
                // --------------------
                conn.createStatement().use { stmt ->
                    var sql = "SELECT * FROM comment_to_person ctp JOIN comment ON (ctp.START_ID = ID) ORDER BY ctp.END_ID"
                    if (limit != null) sql += " LIMIT $limit"
                    val rs = stmt.executeQuery(sql)
                    var prevId: Long? = null
                    var ts: TS? = null
                    while (rs.next()) {
                        val messageId = rs.getLong("id")
                        val personId = rs.getLong("END_ID")
                        if (prevId != personId) {
                            prevId = personId
                            ts = g.getTSM().addTS(personId)
                        }
                        val timestamp = rs.getLong("creationDate")
                        val n = N(messageId, Labels.Comment, timestamp, timestamp, timestamp, g = g,
                            properties = mutableListOf(
                                P(DUMMY_ID, messageId, NODE, "id", messageId, PropType.LONG, timestamp, timestamp, g = g),
                                P(DUMMY_ID, messageId, NODE, "creationDate", rs.getLong("creationDate"), PropType.LONG, timestamp, timestamp, g = g),
                                P(DUMMY_ID, messageId, NODE, "locationIP", rs.getString("locationIP"), PropType.STRING, timestamp, timestamp, g = g),
                                P(DUMMY_ID, messageId, NODE, "browserUsed", rs.getString("browserUsed"), PropType.STRING, timestamp, timestamp, g = g),
                                P(DUMMY_ID, messageId, NODE, "content", rs.getString("content"), PropType.STRING, timestamp, timestamp, g = g),
                                P(DUMMY_ID, messageId, NODE, "length", rs.getInt("length"), PropType.INT, timestamp, timestamp, g = g)
                            ),
                            edges = mutableListOf()
                        )
                        ts!!.add(n, isUpdate = false)
                    }
                }
            }
            return g
        }
    }

    @Test
    fun testSelect() {
        val pattern = listOf(Step(Labels.Person, alias = "p"), null, Step(Labels.Comment, alias = "c"))
        assertTrue(query(g!!, pattern, timeaware = true).isNotEmpty())
    }

//    @Test
//    fun testGroupBy() {
//        val result =
//            query(g!!,
//                match = listOf(Step(Labels.Person, alias = "p"), null, Step(Labels.Comment, alias = "c")),
//                by = listOf(Aggregate("p", ID), Aggregate("c", ID, AggOperator.COUNT))
//            )
//        assertEquals(listOf(), result, message = result.toString())
//    }

    @Test
    fun testGroupBy2() {
        val result =
            query(g!!,
                match = listOf(Step(Labels.Person, alias = "p"), null, Step(Labels.Comment, alias = "c")),
                by = listOf(Aggregate("c", ID, AggOperator.COUNT))
            )
        assertEquals(listOf(cLimit), result, message = result.toString())
    }
}