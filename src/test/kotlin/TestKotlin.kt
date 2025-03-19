import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.interfaces.TS
import it.unibo.graph.query.*
import it.unibo.graph.rocksdb.RocksDBGraph
import it.unibo.graph.structure.CustomGraph
import it.unibo.graph.utils.*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.test.Test
import kotlin.test.assertTrue

class TestKotlin {

    fun matrix(f: (Graph) -> Unit) {
        listOf(/*MemoryGraph(), */RocksDBGraph())
            .map { setup(it) }
            .forEach {
                f(it)
                it.close()
            }
    }

    fun setup(): CustomGraph {
        return setup(MemoryGraph())
    }

    fun setup(g: Graph): CustomGraph {
        val g = CustomGraph(g)
        g.tsm = AsterixDBTSM.createDefault(g)
        g.clear()
        g.getTSM().clear()

        val n0 = g.addNode("AgriFarm")
        val n1 = g.addNode("AgriParcel")
        val n2 = g.addNode("Device")
        val n3 = g.addNode("Device")
        val n4 = g.addNode("Person")
        val n5 = g.addNode("Person")

        g.addProperty(n0.id, "name", "Errano", PropType.STRING)
        g.addProperty(n0.id, "location", "{\"coordinates\":[[[11.798105,44.234354],[11.801217,44.237683],[11.805286,44.235809],[11.803987,44.234851],[11.804789,44.233683],[11.80268,44.231419],[11.798105,44.234354]]],\"type\":\"Polygon\"}\n", PropType.GEOMETRY)
        g.addProperty(n1.id, "name", "T0", PropType.STRING)
        g.addProperty(n1.id, "location", "{\"coordinates\":[[[11.79915,44.235384],[11.799412,44.23567],[11.801042,44.234555],[11.800681,44.234343],[11.79915,44.235384]]],\"type\":\"Polygon\"}", PropType.GEOMETRY)
        g.addProperty(n2.id, "name", "GB", PropType.STRING)
        g.addProperty(n2.id, "location", "{\"coordinates\":[11.799328,44.235394],\"type\":\"Point\"}", PropType.GEOMETRY)
        g.addProperty(n3.id, "location", "{\"coordinates\":[14.800711,44.234904],\"type\":\"Point\"}", PropType.GEOMETRY)

        g.addEdge("hasParcel", n0.id, n1.id)
        g.addEdge("hasDevice", n0.id, n2.id)
        g.addEdge("hasDevice", n1.id, n2.id)
        g.addEdge("foo", n1.id, n1.id)
        g.addEdge("hasDevice", n1.id, n3.id)
        g.addEdge("hasOwner", n2.id, n4.id)
        g.addEdge("hasOwner", n3.id, n4.id)
        g.addEdge("hasManutentor", n2.id, n5.id)
        g.addEdge("hasManutentor", n3.id, n5.id)
        g.addEdge("hasFriend", n4.id, n5.id)

        var timestamp = 0L

        val ts1: TS = g.getTSM().addTS()
        val m1 = ts1.add("Measurement", timestamp = timestamp++, value = 10)
        ts1.add("Measurement", timestamp = timestamp++, value = 11)
        ts1.add("Measurement", timestamp = timestamp++, value = 12)
        val n6 = g.addNode("Humidity", value = ts1.getTSId())
        g.addEdge("hasHumidity", n3.id, n6.id)
        g.addEdge("hasOwner", m1.id, n4.id, id = DUMMY_ID)
        g.addEdge("hasManutentor", m1.id, n4.id, id = DUMMY_ID)
        g.addProperty(m1.id, "unit", "Celsius", PropType.STRING, id = DUMMY_ID)

        val ts2 = g.getTSM().addTS()
        ts2.add("Measurement", timestamp = timestamp++, value = 10)
        ts2.add("Measurement", timestamp = timestamp++, value = 11)
        ts2.add("Measurement", timestamp = timestamp++, value = 12)
        val n7 = g.addNode("Temperature", value = ts2.getTSId())
        g.addEdge("hasTemperature", n3.id, n7.id)

        ts1.add("Measurement", timestamp = timestamp++, value = 13)
        ts1.add("Measurement", timestamp = timestamp++, value = 14)
        ts1.add("Measurement", timestamp = timestamp++, value = 15)

        val ts3 = g.getTSM().addTS()
        val m2 = ts3.add("Measurement", timestamp = timestamp++, value = 23)
        ts3.add("Measurement", timestamp = timestamp++, value = 24)
        ts3.add("Measurement", timestamp = timestamp, value = 25)
        val n8 = g.addNode("SolarRadiation", value = ts3.getTSId())
        g.addEdge("hasSolarRadiation", n2.id, n8.id)
        g.addEdge("hasManutentor", m2.id, n4.id, id = DUMMY_ID)

        return g
    }

    fun setup1(): CustomGraph {
        val g = CustomGraph(MemoryGraph())
        g.tsm = AsterixDBTSM.createDefault(g)
        g.clear()
        g.getTSM().clear()

        val n10 = g.addNode("A")
        val n11 = g.addNode("B")
        val n12 = g.addNode("C")
        val n20 = g.addNode("A")
        val n21 = g.addNode("B")
        val n22 = g.addNode("C")

        val n13 = g.addNode("D")
        val n14 = g.addNode("E")
        val n15 = g.addNode("F")
        val n23 = g.addNode("D")
        val n24 = g.addNode("E")
        val n25 = g.addNode("F")

        g.addProperty(n10.id, "name", "Errano", PropType.STRING)
        g.addProperty(n10.id, "lastname", "Errano", PropType.STRING)
        g.addProperty(n11.id, "name", "Bar", PropType.STRING)
        g.addProperty(n13.id, "name", "Errano", PropType.STRING)
        g.addProperty(n14.id, "lastname", "Errano", PropType.STRING)

        g.addEdge("foo", n10.id, n11.id)
        g.addEdge("foo", n11.id, n12.id)
        g.addEdge("foo", n13.id, n14.id)
        g.addEdge("foo", n14.id, n15.id)
        g.addEdge("foo", n20.id, n21.id)
        g.addEdge("foo", n21.id, n22.id)
        g.addEdge("foo", n23.id, n24.id)
        g.addEdge("foo", n24.id, n25.id)

        return g
    }

    @Test
    fun testSum() {
        assertEquals(42, 40 + 2)
    }

    @Test
    fun testID() {
        val n = 100L
        assertEquals(Pair(n, n), decodeBitwise(encodeBitwise(n, n)))
        val ts = System.currentTimeMillis()
        assertEquals(Pair(n, ts), decodeBitwise(encodeBitwise(n, ts)))
    }

    @Test
    fun testRel() {
        matrix { g ->
            listOf(
                Pair(g.getNode(N0), 2),
                Pair(g.getNode(N1), 4),
                Pair(g.getNode(N2), 5),
                Pair(g.getNode(N3), 5),
                Pair(g.getNode(N4), 3)
            ).forEach {
                assertEquals(it.second, it.first.getRels().size, it.first.getRels().toString())
            }
        }
    }

    @Test
    fun testProps() {
        matrix { g ->
            val m1 = g.getTSM().getTS(1).get(0)
            listOf(Pair(g.getNode(N0), 2), Pair(g.getNode(N1), 2), Pair(g.getNode(N2), 2), Pair(m1, 1)).forEach {
                assertEquals(it.second, it.first.getProps().size, it.first.getProps().toString())
            }
        }
    }

    @Test
    fun testTS() {
        matrix { g ->
            listOf(Pair(g.getNode(N6), 6), Pair(g.getNode(N7), 3), Pair(g.getNode(N8), 3)).forEach {
                assertTrue(it.first.value != null)
                assertEquals(it.second, it.first.getTS().size, it.first.getTS().toString())
            }
        }
    }

    @Test
    fun testTSAsNode0() {
        val g = GraphTraversalSource(setup())
        kotlin.test.assertEquals(
            1, g.V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .toList().size
        )
    }

    @Test
    fun testSearch0() {
        matrix { g ->
            kotlin.test.assertEquals(1, search(g, listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"))).size)
        }
    }

    @Test
    fun testJoin() {
        val g = setup1()
        var patterns = listOf(
            listOf(Step("A", alias = "a"), null, Step("B"), null, Step("C")),
            listOf(Step("D", alias = "d"), null, Step("E"), null, Step("F"))
        )
        kotlin.test.assertEquals(1, query(g, patterns, where = listOf(Compare("a", "d", "name", Operators.EQ))).size)
        kotlin.test.assertEquals(1, query(g, patterns, where = listOf(Compare("d", "a", "name", Operators.EQ))).size)
        patterns = listOf(
            listOf(Step("A", alias = "a"), null, Step("B"), null, Step("C")),
            listOf(Step("D"), null, Step("E", alias = "d"), null, Step("F"))
        )
        kotlin.test.assertEquals(1, query(g, patterns, where = listOf(Compare("a", "d", "lastname", Operators.EQ))).size)
        kotlin.test.assertEquals(1, query(g, patterns, where = listOf(Compare("d", "a", "lastname", Operators.EQ))).size)
        kotlin.test.assertEquals(
            listOf(listOf("Errano", "Errano", "Errano")),
            query(g, 
                listOf(
                    listOf(Step("A", alias = "a"), null, Step("B"), null, Step("C")),
                    listOf(Step("D", alias = "d"), null, Step("E", alias = "e"), null, Step("F"))
                ),
                where = listOf(Compare("e", "a", "lastname", Operators.EQ)),
                by = listOf(Aggregate("a", property = "name"), Aggregate("d", property = "name"), Aggregate("e", property = "lastname"))
            )
        )
    }

    @Test
    fun testJoin1() {
        val g = setup1()
        val res =
            query(g, 
                listOf(
                    listOf(Step("A", alias="a"), null, Step("B"), null, Step("C")),
                    listOf(Step("D"), null, Step("E"), null, Step("F"))
                )
            )
        kotlin.test.assertEquals(
            4,
            res.size,
            res.toString()
        )
    }

    @Test
    fun testTSAsNode1() {
        val g = GraphTraversalSource(setup())
        kotlin.test.assertEquals(
            6, g.V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .out(HAS_TS).toList().size
        )
    }

    @Test
    fun testSearch1() {
        val g = setup()
        kotlin.test.assertEquals(1, search(g, listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS))).size)
    }

    @Test
    fun testSearch2() {
        val g = setup()
        val pattern = listOf(Step("AgriFarm"), Step("hasParcel"), Step("AgriParcel", alias = "p"), Step("hasDevice"), Step("Device", alias = "d"))
        kotlin.test.assertEquals(2, search(g, pattern).size)
        kotlin.test.assertEquals(1, search(g, pattern, listOf(Compare("p", "d", "location", Operators.ST_CONTAINS))).size)
    }

    @Test
    fun testSearchTS() {
        val g = setup()
        val pattern = listOf(Step("AgriFarm"), Step("hasParcel"), Step("AgriParcel", alias = "p"), Step("hasDevice"), Step("Device"), Step("hasSolarRadiation"), Step("SolarRadiation"), Step(HAS_TS), Step("Measurement", alias = "m"))
        kotlin.test.assertEquals(3, search(g, pattern, listOf(Compare("p", "m", "location", Operators.ST_CONTAINS))).size)
    }

    @Test
    fun testSearch1bis() {
        val g = setup()
        kotlin.test.assertEquals(6, search(g, listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS), Step("Measurement"))).size)
        kotlin.test.assertEquals(1, search(g, listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS), Step("Measurement", listOf(Triple("unit", Operators.EQ, "Celsius"))))).size)
    }

    @Test
    fun testTSAsNode4() {
        val g = setup()
        kotlin.test.assertEquals(
            listOf(12.5), GraphTraversalSource(g).V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .out(HAS_TS)
                .values<Number>(VALUE)
                .mean<Number>()
                .toList()
        )

        kotlin.test.assertEquals(
            listOf(12.5 as Any),
            query(g, 
                listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS), Step("Measurement", alias = "m")),
                by = listOf(Aggregate("m", "value", AggOperator.AVG))
            )
        )

        kotlin.test.assertEquals(
            listOf(15.0 as Any),
            query(g, 
                listOf(Step("AgriParcel"), null, Step("Device"), null, null, Step(HAS_TS), Step("Measurement", alias = "m")),
                by = listOf(Aggregate("m", "value", AggOperator.AVG))
            )
        )
    }

    @Test
    fun testReturn() {
        val g = setup1()
        kotlin.test.assertEquals(
            listOf(listOf("Errano", "Bar"), listOf("null", "null")),
            query(g, listOf(Step("A", alias = "n"), null, Step("B", alias = "m")), by = listOf(Aggregate("n", "name"), Aggregate("m", "name")))
        )

        kotlin.test.assertEquals(
            listOf("Errano" as Any, "null" as Any),
            query(g, listOf(Step("A", alias = "n")), by = listOf(Aggregate("n", "name")))
        )
    }

    @Test
    fun testSearch5() {
        val g = setup()
        kotlin.test.assertEquals(12, search(g, listOf(null, Step(HAS_TS), Step("Measurement"))).size)
    }

    @Test
    fun testTSAsNode5() {
        val g = GraphTraversalSource(setup())
        kotlin.test.assertEquals(
            listOf(24.0), g.V()
                .hasLabel("Device")
                .out("hasSolarRadiation")
                .hasLabel("SolarRadiation")
                .out(HAS_TS)
                .values<Number>(VALUE)
                .mean<Number>()
                .toList()
        )
    }

    @Test
    fun testTSAsNode2() {
        val g = GraphTraversalSource(setup())
        kotlin.test.assertEquals(
            1, g.V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .out(HAS_TS)
                .out("hasManutentor").toList().size
        )
    }

    @Test
    fun testTSAsNode3() {
        val g = GraphTraversalSource(setup())
        kotlin.test.assertEquals(
            1, g.V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .out(HAS_TS)
                .out("hasManutentor")
                .out("hasFriend").toList().size
        )
    }
}