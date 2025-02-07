import it.unibo.graph.App
import it.unibo.graph.N
import it.unibo.graph.PropType
import it.unibo.graph.structure.CustomEdge
import it.unibo.graph.structure.CustomVertex
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

    @BeforeTest
    fun setup() {
        g.clear()

        n1 = g.addNode("AgriFarm")
        n4 = g.addNode("Device")
        n2 = g.addNode("AgriParcel")
        n3 = g.addNode("Device")
        n5 = g.addNode("Person")
        val n6 = g.addNode("Person")

        g.addProperty(n1!!.id, "name", "Errano", PropType.STRING)
        g.addProperty(n1!!.id, "location", "GeoJSON", PropType.GEOMETRY)
        g.addProperty(n2!!.id, "name", "T0", PropType.STRING)
        g.addProperty(n2!!.id, "location", "GeoJSON", PropType.GEOMETRY)
        g.addProperty(n3!!.id, "name", "GB", PropType.STRING)
        g.addProperty(n3!!.id, "location", "GeoJSON", PropType.GEOMETRY)

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

        val ts1 = g.addTS()
        val m1 = CustomVertex(ts1.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 10)
        ts1.add(m1)
        ts1.add(CustomVertex(ts1.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 11))
        ts1.add(CustomVertex(ts1.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 12))
        n7 = g.addNode("Humidity", value = ts1.id.toLong())
        g.addEdge("hasHumidity", n4!!.id, n7!!.id)
        m1.relationships += CustomEdge(-1, "hasOwner", n7!!.id, n5!!.id)
        m1.relationships += CustomEdge(-1, "hasManutentor", n7!!.id, n5!!.id)

        val ts2 = g.addTS()
        ts2.add(CustomVertex(ts2.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 10))
        ts2.add(CustomVertex(ts2.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 11))
        ts2.add(CustomVertex(ts2.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 12))
        n8 = g.addNode("Temperature", value = ts2.id.toLong())
        g.addEdge("hasTemperature", n4!!.id, n8!!.id)

        ts1.add(CustomVertex(ts1.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 13))
        ts1.add(CustomVertex(ts1.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 14))
        ts1.add(CustomVertex(ts1.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 15))

        val ts3 = g.addTS()
        ts3.add(CustomVertex(ts3.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 23))
        ts3.add(CustomVertex(ts3.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 24))
        ts3.add(CustomVertex(ts3.values.size, "Measurement", timestamp = System.currentTimeMillis(), value = 25))
        n9 = g.addNode("SolarRadiation", value = ts3.id.toLong())
        g.addEdge("hasSolarRadiation", n5!!.id, n9!!.id)
    }

    @Test
    fun testSum() {
        assertEquals(42, 40 + 2)
    }

//    @Test
//    fun putAndGet() {
//        val n = g.addNode("foo")
//        val p = g.addProperty(n.id, "name", "GB", PropType.STRING)
//        val m = g.addNode("bar")
//        val r = g.addEdge("foo", n.id, m.id)
//        assertEquals(n, g.getNode(n.id), g.getNode(n.id).toString())
//        assertEquals(r, g.getEdge(r.id), g.getEdge(r.id).toString())
//        assertEquals(p, g.getProp(p.id), g.getProp(p.id).toString())
//    }

    @Test
    fun testRel() {
        listOf(
            Pair(g.getNode(n1!!.id), 2),
            Pair(g.getNode(n2!!.id), 4),
            Pair(g.getNode(n3!!.id), 4),
            Pair(g.getNode(n4!!.id), 5),
            Pair(g.getNode(n5!!.id), 4)
        ).forEach {
            assertEquals(it.second, it.first.getRels().size, it.first.getRels().toString())
        }
    }

    @Test
    fun testProps() {
        listOf(Pair(g.getNode(n1!!.id), 2), Pair(g.getNode(n2!!.id), 2), Pair(g.getNode(n3!!.id), 2)).forEach {
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
    fun tstTSAsNode() {
        val g = GraphTraversalSource(g)
        kotlin.test.assertEquals(
            6, g.V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .out("hasTS").toList().size
        )
    }

    @Test
    fun tstTSAsNode4() {
        val g = GraphTraversalSource(g)
        kotlin.test.assertEquals(
            listOf(12.5), g.V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .out("hasTS")
                .values<Number>("value")
                .mean<Number>()
                .toList()
        )
    }

    @Test
    fun tstTSAsNode2() {
        val g = GraphTraversalSource(g)
        kotlin.test.assertEquals(
            1, g.V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .out("hasTS")
                .out("hasManutentor").toList().size
        )
    }

    @Test
    fun tstTSAsNode3() {
        val g = GraphTraversalSource(g)
        kotlin.test.assertEquals(
            1, g.V()
                .hasLabel("Device")
                .out("hasHumidity")
                .hasLabel("Humidity")
                .out("hasTS")
                .out("hasManutentor")
                .out("hasFriend").toList().size
        )
    }
}