import it.unibo.graph.Graph
import it.unibo.graph.N
import it.unibo.graph.PropType
import it.unibo.graph.TS
import it.unibo.graph.structure.CustomEdge
import it.unibo.graph.structure.CustomGraph
import it.unibo.graph.structure.CustomVertex
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue


class TestKotlin {

    val graph = CustomGraph()
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
        Graph.clear()

        n1 = Graph.addNode2("AgriFarm", graph)
        n4 = Graph.addNode2("Device", graph)
        n2 = Graph.addNode2("AgriParcel", graph)
        n3 = Graph.addNode2("Device", graph)
        n5 = Graph.addNode2("Person", graph)
        val n6 = Graph.addNode2("Person", graph)


        Graph.addProperty2(n1!!.id, "name", "Errano", PropType.STRING)
        Graph.addProperty2(n1!!.id, "location", "GeoJSON", PropType.GEOMETRY)
        Graph.addProperty2(n2!!.id, "name", "T0", PropType.STRING)
        Graph.addProperty2(n2!!.id, "location", "GeoJSON", PropType.GEOMETRY)
        Graph.addProperty2(n3!!.id, "name", "GB", PropType.STRING)
        Graph.addProperty2(n3!!.id, "location", "GeoJSON", PropType.GEOMETRY)

        Graph.addRel2("hasParcel", n1!!.id, n2!!.id, graph)
        Graph.addRel2("hasDevice", n1!!.id, n3!!.id, graph)
        Graph.addRel2("hasDevice", n2!!.id, n3!!.id, graph)
        Graph.addRel2("foo", n2!!.id, n2!!.id, graph)
        Graph.addRel2("hasDevice", n2!!.id, n4!!.id, graph)
        Graph.addRel2("hasOwner", n3!!.id, n5!!.id, graph)
        Graph.addRel2("hasOwner", n4!!.id, n5!!.id, graph)
        Graph.addRel2("hasManutentor", n3!!.id, n6.id, graph)
        Graph.addRel2("hasManutentor", n4!!.id, n6.id, graph)
        Graph.addRel2("hasFriend", n5!!.id, n6.id, graph)

        val ts1 = TS(Graph.ts.size)
        Graph.ts += ts1

        val m1 = CustomVertex(ts1.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 10)
        ts1.add(m1)
        ts1.add(CustomVertex(ts1.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 11))
        ts1.add(CustomVertex(ts1.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 12))
        n7 = Graph.addNode2("Humidity", graph, value = ts1.id.toLong())
        Graph.addRel2("hasHumidity", n4!!.id, n7!!.id, graph)
        m1.relationships += CustomEdge(-1, "hasOwner", n7!!.id, n5!!.id, graph)
        m1.relationships += CustomEdge(-1, "hasManutentor", n7!!.id, n5!!.id, graph)

        val ts2 = TS(Graph.ts.size)
        Graph.ts += ts2
        ts2.add(CustomVertex(ts2.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 10))
        ts2.add(CustomVertex(ts2.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 11))
        ts2.add(CustomVertex(ts2.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 12))
        n8 = Graph.addNode2("Temperature", graph, value = ts2.id.toLong())
        Graph.addRel2("hasTemperature", n4!!.id, n8!!.id, graph)

        ts1.add(CustomVertex(ts1.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 13))
        ts1.add(CustomVertex(ts1.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 14))
        ts1.add(CustomVertex(ts1.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 15))

        val ts3 = TS(Graph.ts.size)
        Graph.ts += ts3
        ts3.add(CustomVertex(ts3.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 23))
        ts3.add(CustomVertex(ts3.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 24))
        ts3.add(CustomVertex(ts3.values.size, "Measurement", graph, timestamp = System.currentTimeMillis(), value = 25))
        n9 = Graph.addNode2("SolarRadiation", graph, value = ts3.id.toLong())
        Graph.addRel2("hasSolarRadiation", n5!!.id, n9!!.id, graph)
    }

    @Test
    fun testSum() {
        assertEquals(42, 40 + 2)
    }

    @Test
    fun testRel() {
        listOf(Pair(n1!!, 2), Pair(n2!!, 4), Pair(n3!!, 4), Pair(n4!!, 5), Pair(n5!!, 4)).forEach {
            assertEquals(it.second, it.first.getRels().size, it.first.getRels().toString())
        }
    }

    @Test
    fun testProps() {
        listOf(Pair(n1!!, 2), Pair(n2!!, 2), Pair(n3!!, 2)).forEach {
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
        val g = GraphTraversalSource(graph)
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
        val g = GraphTraversalSource(graph)
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
        val g = GraphTraversalSource(graph)
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
        val g = GraphTraversalSource(graph)
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

    @Test
    fun testTinkerPop() {
        Graph.clear()
        val graph = CustomGraph()
        val alice: Vertex = graph.addVertex("label", "Person", "name", "Alice")
        val bob: Vertex = graph.addVertex("label", "Person", "name", "Bob")
        alice.addEdge("knows", bob)
        val g = GraphTraversalSource(graph)
        kotlin.test.assertEquals(2, g.V().hasLabel("Person").toList().size)
        kotlin.test.assertEquals(1, g.V().hasLabel("Person").has("name", "Alice").toList().size)
        kotlin.test.assertEquals(listOf("Bob"), g.V().hasLabel("Person").out("knows").values<String>("name").toList())
        kotlin.test.assertEquals(
            listOf("Alice"),
            g.V().hasLabel("Person").`in`("knows").values<String>("name").toList()
        )
    }
}