import it.unibo.graph.*
import it.unibo.graph.App.tsm
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue


class TestKotlin {

    val g = App.g
    var n1: N? = null
    var n2: N? = null
    var n3: N? = null
    var n4: N? = null
    var n5: N? = null
    var n7: N? = null
    var n8: N? = null
    var n9: N? = null

    val initialTimestamp = System.currentTimeMillis()

    @BeforeTest
    fun setup() {
        g.clear()
        tsm.clear()

        n1 = g.addNode("AgriFarm")
        n4 = g.addNode("Device")
        n2 = g.addNode("AgriParcel")
        n3 = g.addNode("Device")
        n5 = g.addNode("Person")
        val n6 = g.addNode("Person")

        g.addProperty(n1!!.id, "name", "Errano", PropType.STRING)
        g.addProperty(n1!!.id, "location", "{\"coordinates\":[[[11.798105,44.234354],[11.801217,44.237683],[11.805286,44.235809],[11.803987,44.234851],[11.804789,44.233683],[11.80268,44.231419],[11.798105,44.234354]]],\"type\":\"Polygon\"}\n", PropType.GEOMETRY)
        g.addProperty(n2!!.id, "name", "T0", PropType.STRING)
        g.addProperty(n2!!.id, "location", "{\"coordinates\":[[[11.79915,44.235384],[11.799412,44.23567],[11.801042,44.234555],[11.800681,44.234343],[11.79915,44.235384]]],\"type\":\"Polygon\"}", PropType.GEOMETRY)
        g.addProperty(n3!!.id, "name", "GB", PropType.STRING)
        g.addProperty(n3!!.id, "location", "{\"coordinates\":[11.799328,44.235394],\"type\":\"Point\"}", PropType.GEOMETRY)
        g.addProperty(n4!!.id, "location", "{\"coordinates\":[14.800711,44.234904],\"type\":\"Point\"}", PropType.GEOMETRY)

        g.addEdge("hasParcel", n1!!.id, n2!!.id)
        g.addEdge("hasDevice", n1!!.id, n3!!.id)
        g.addEdge("hasDevice", n2!!.id, n3!!.id)
        g.addEdge("foo", n2!!.id, n2!!.id)
        g.addEdge("hasDevice", n2!!.id, n4!!.id)
        g.addEdge("hasOwner", n3!!.id, n5!!.id)
        g.addEdge("hasOwner", n4!!.id, n5!!.id)
        g.addEdge("hasManutentor", n3!!.id, n6.id)
        g.addEdge("hasManutentor", n4!!.id, n6.id)
        g.addEdge("hasFriend", n5!!.id, n6.id)

        var timestamp = 0L

        val ts1: TS = tsm.addTS()
        val m1 = ts1.add("Measurement", timestamp = timestamp++, value = 10)
        ts1.add("Measurement", timestamp = timestamp++, value = 11)
        ts1.add("Measurement", timestamp = timestamp++, value = 12)
        n7 = g.addNode("Humidity", value = ts1.getTSId())
        g.addEdge("hasHumidity", n4!!.id, n7!!.id)
        g.addEdge("hasOwner", m1.id, n5!!.id, id = DUMMY_ID)
        g.addEdge("hasManutentor", m1.id, n5!!.id, id = DUMMY_ID)
        g.addProperty(m1.id, "unit", "Celsius", PropType.STRING, id = DUMMY_ID)

        val ts2 = tsm.addTS()
        ts2.add("Measurement", timestamp = timestamp++, value = 10)
        ts2.add("Measurement", timestamp = timestamp++, value = 11)
        ts2.add("Measurement", timestamp = timestamp++, value = 12)
        n8 = g.addNode("Temperature", value = ts2.getTSId())
        g.addEdge("hasTemperature", n4!!.id, n8!!.id)

        ts1.add("Measurement", timestamp = timestamp++, value = 13)
        ts1.add("Measurement", timestamp = timestamp++, value = 14)
        ts1.add("Measurement", timestamp = timestamp++, value = 15)

        val ts3 = tsm.addTS()
        val m2 = ts3.add("Measurement", timestamp = timestamp++, value = 23)
        ts3.add("Measurement", timestamp = timestamp++, value = 24)
        ts3.add("Measurement", timestamp = timestamp, value = 25)
        n9 = g.addNode("SolarRadiation", value = ts3.getTSId())
        g.addEdge("hasSolarRadiation", n3!!.id, n9!!.id)
        g.addEdge("hasManutentor", m2.id, n5!!.id, id = DUMMY_ID)

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
        g.addProperty(n11.id, "name", "TO", PropType.STRING)
        g.addProperty(n12.id, "location", "{\"coordinates\":[11.799328,44.235394],\"type\":\"Point\"}", PropType.GEOMETRY)

        g.addProperty(n13.id, "name", "Errano", PropType.STRING)
        g.addProperty(n14.id, "name", "TO", PropType.STRING)
        g.addProperty(n15.id, "location", "{\"coordinates\":[11.799328,44.235394],\"type\":\"Point\"}", PropType.GEOMETRY)

        g.addEdge("foo", n10.id, n11.id)
        g.addEdge("foo", n11.id, n12.id)
        g.addEdge("foo", n13.id, n14.id)
        g.addEdge("foo", n14.id, n15.id)
        g.addEdge("foo", n20.id, n21.id)
        g.addEdge("foo", n21.id, n22.id)
        g.addEdge("foo", n23.id, n24.id)
        g.addEdge("foo", n24.id, n25.id)
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
        listOf(
            Pair(g.getNode(n1!!.id), 2),
            Pair(g.getNode(n2!!.id), 4),
            Pair(g.getNode(n3!!.id), 5),
            Pair(g.getNode(n4!!.id), 5),
            Pair(g.getNode(n5!!.id), 3)
        ).forEach {
            assertEquals(it.second, it.first.getRels().size, it.first.getRels().toString())
        }
    }

    @Test
    fun testProps() {
        val m1 = tsm.getTS(1).get(0)
        listOf(Pair(g.getNode(n1!!.id), 2), Pair(g.getNode(n2!!.id), 2), Pair(g.getNode(n3!!.id), 2), Pair(m1, 1)).forEach {
            assertEquals(it.second, it.first.getProps().size, it.first.getProps().toString())
        }
    }

    @Test
    fun testTS() {
        listOf(Pair(n7!!, 6), Pair(n8!!, 3), Pair(n9!!, 3)).forEach {
            assertTrue(it.first.value != null)
            assertEquals(it.second, it.first.getTS().size, it.first.getTS().toString())
        }
    }

    @Test
    fun testTSAsNode0() {
        val g = GraphTraversalSource(g)
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
        kotlin.test.assertEquals(1, search(listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"))).size)
    }

    @Test
    fun testJoin() {
        val res =
            join(
                listOf(
                    listOf(Step("A", alias="a"), null, Step("B"), null, Step("C")),
                    listOf(Step("D", alias="d"), null, Step("E"), null, Step("F"))
                ),
                where = listOf(Compare("a", "d", "name", Operators.EQ)),
                by = listOf(),
                agg = null
            )
        kotlin.test.assertEquals(
            1,
            res.size
        )
    }

    @Test
    fun testJoin1() {
        val res =
            join(
                listOf(
                    listOf(Step("A", alias="a"), null, Step("B"), null, Step("C")),
                    listOf(Step("D"), null, Step("E"), null, Step("F"))
                ),
                by = listOf(),
                agg = null
            )
        kotlin.test.assertEquals(
            4,
            res.size
        )
    }

    @Test
    fun testTSAsNode1() {
        val g = GraphTraversalSource(g)
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
        kotlin.test.assertEquals(1, search(listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS))).size)
    }

    @Test
    fun testSearch2() {
        val pattern = listOf(Step("AgriFarm"), Step("hasParcel"), Step("AgriParcel", alias = "p"), Step("hasDevice"), Step("Device", alias = "d"))
        kotlin.test.assertEquals(2, search(pattern).size)
        kotlin.test.assertEquals(1, search(pattern, listOf(Compare("p", "d", "location", Operators.ST_CONTAINS))).size)
    }

    @Test
    fun testSearchTS() {
        val pattern = listOf(Step("AgriFarm"), Step("hasParcel"), Step("AgriParcel", alias = "p"), Step("hasDevice"), Step("Device"), Step("hasSolarRadiation"), Step("SolarRadiation"), Step(HAS_TS), Step("Measurement", alias = "m"))
        kotlin.test.assertEquals(3, search(pattern, listOf(Compare("p", "m", "location", Operators.ST_CONTAINS))).size)
    }

    @Test
    fun testSearch1bis() {
        kotlin.test.assertEquals(6, search(listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS), Step("Measurement"))).size)
        kotlin.test.assertEquals(1, search(listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS), Step("Measurement", listOf(Triple("unit", Operators.EQ, "Celsius"))))).size)
    }

    @Test
    fun testTSAsNode4() {
        val g = GraphTraversalSource(g)
        kotlin.test.assertEquals(
            listOf(12.5), g.V()
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
            search(
                listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS), Step("Measurement", alias = "m")),
                where = listOf(),
                by = listOf(),
                Aggregate("m", "value", AggOperator.AVG)
            )
        )

        kotlin.test.assertEquals(
            listOf(15.0 as Any),
            search(
                listOf(Step("AgriParcel"), null, Step("Device"), null, null, Step(HAS_TS), Step("Measurement", alias = "m")),
                where = listOf(),
                by = listOf(),
                Aggregate("m", "value", AggOperator.AVG)
            )
        )
    }

    @Test
    fun testSearch4() {
        kotlin.test.assertEquals(75, search(listOf(Step("Device"), Step("hasHumidity"), Step("Humidity"), Step(HAS_TS), Step("Measurement"))).map { (it.last() as N).value }.sumOf { it!! })
    }

    @Test
    fun testSearch5() {
        kotlin.test.assertEquals(12, search(listOf(null, Step(HAS_TS), Step("Measurement"))).size)
    }

    @Test
    fun testTSAsNode5() {
        val g = GraphTraversalSource(g)
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
        val g = GraphTraversalSource(g)
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
        val g = GraphTraversalSource(g)
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