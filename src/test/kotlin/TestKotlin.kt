import it.unibo.graph.*
import it.unibo.graph.structure.CustomGraph
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.jupiter.api.Assertions.*
import kotlin.test.BeforeTest
import kotlin.test.Test


class TestKotlin {

    var n1: N? = null
    var n2: N? = null
    var n3: N? = null

    @BeforeTest
    fun setup() {
        Graph.clear()

        val n4 = Graph.addNode("Device")
        n1 = Graph.addNode("AgriFarm")
        n2 = Graph.addNode("AgriParcel")
        n3 = Graph.addNode("Device")
        val n5 = Graph.addNode("Person")
        val n6 = Graph.addNode("Person")

        Graph.addProperty(n1!!.id, "name", "Errano", PropType.STRING)
        Graph.addProperty(n1!!.id, "location", "GeoJSON", PropType.GEOMETRY)
        Graph.addProperty(n2!!.id, "name", "T0", PropType.STRING)
        Graph.addProperty(n2!!.id, "location", "GeoJSON", PropType.GEOMETRY)
        Graph.addProperty(n3!!.id, "name", "GB", PropType.STRING)
        Graph.addProperty(n3!!.id, "location", "GeoJSON", PropType.GEOMETRY)

        Graph.addRel("hasParcel", n1!!.id, n2!!.id)
        Graph.addRel("hasDevice", n1!!.id, n3!!.id)
        Graph.addRel("hasDevice", n2!!.id, n3!!.id)
        Graph.addRel("foo", n2!!.id, n2!!.id)
        Graph.addRel("hasDevice", n2!!.id, n4.id)
        Graph.addRel("hasOwner", n3!!.id, n5.id)
        Graph.addRel("hasOwner", n4.id, n5.id)
        Graph.addRel("hasManutentor", n3!!.id, n6.id)
        Graph.addRel("hasManutentor", n4.id, n6.id)

        val ts1 = TS(Graph.ts.size)
        Graph.ts += ts1
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), 10))
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), 11))
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), 12))
        Graph.props += P(Graph.props.size, n1!!.id, "humidity", ts1.id, PropType.TS)

        val ts2 = TS(Graph.ts.size)
        Graph.ts += ts2
        ts2.add(TSelem(ts2.values.size, ts2.id, System.currentTimeMillis(), 10))
        ts2.add(TSelem(ts2.values.size, ts2.id, System.currentTimeMillis(), 11))
        ts2.add(TSelem(ts2.values.size, ts2.id, System.currentTimeMillis(), 12))
        Graph.props += P(Graph.props.size, n2!!.id, "temperature", ts2.id, PropType.TS)

        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), 13))
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), 14))
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), 15))

        val ts3 = TS(Graph.ts.size)
        Graph.ts += ts3
        ts3.add(TSelem(ts3.values.size, ts3.id, System.currentTimeMillis(), 23))
        ts3.add(TSelem(ts3.values.size, ts3.id, System.currentTimeMillis(), 24))
        ts3.add(TSelem(ts3.values.size, ts3.id, System.currentTimeMillis(), 25))
        Graph.props += P(Graph.props.size, n1!!.id, "solarRadiation", ts3.id, PropType.TS)
    }

    @Test
    fun testSum() {
        assertEquals(42, 40 + 2)
    }

    @Test
    fun testRel() {
        listOf(Pair(n1!!, 2), Pair(n2!!, 4), Pair(n3!!, 4)).forEach {
            assertEquals(it.second, it.first.getRels().size, it.first.getRels().toString())
        }
    }

    @Test
    fun testProps() {
        listOf(Pair(n1!!, 4), Pair(n2!!, 3), Pair(n3!!, 2)).forEach {
            assertEquals(it.second, it.first.getProps().size, it.first.getProps().toString())
        }
    }

    @Test
    fun testTS() {
        listOf(Triple(n1!!, 2, 3), Triple(n2!!, 1, 3), Triple(n3!!, 0, 0)).forEach {
            assertEquals(it.second, it.first.getTS().size, it.first.getTS().toString())
            if (it.second > 0) {
                assertEquals(it.third, it.first.getTS()[0].second.size, it.first.getTS()[0].second.toString())
            }
        }
    }

    fun pattern2string(patterns: List<List<N>>): String {
        return patterns.joinToString("\n") { pattern -> pattern.joinToString(", ") { node -> node.toString() } }
    }

    fun printPatterns(patterns: List<List<N>>) {
        println(pattern2string(patterns))
    }

    @Test
    fun testSearch1() {
        val patterns = Graph.search(listOf("AgriParcel"))
        assertEquals(1, patterns.size, pattern2string(patterns))
    }

    @Test
    fun testSearch2() {
        val patterns = Graph.search(listOf("AgriFarm", "AgriParcel"))
        assertEquals(1, patterns.size, pattern2string(patterns))
    }

    @Test
    fun testSearch3() {
        val patterns = Graph.search(listOf("AgriFarm", "AgriParcel", "Device"))
        assertEquals(2, patterns.size, pattern2string(patterns))
    }

    @Test
    fun testSearch4() {
        val patterns = Graph.search(listOf("AgriFarm", "Device"))
        assertEquals(1, patterns.size, pattern2string(patterns))
    }

    @Test
    fun testSearch5() {
        val patterns = Graph.search(listOf("AgriFarm", "AgriParcel", "Device", "Person"))
        assertEquals(4, patterns.size, pattern2string(patterns))
    }

    @Test
    fun testTinkerPop() {
        Graph.clear()
        val graph = CustomGraph()
        val alice: Vertex = graph.addVertex("label", "Person", "name", "Alice")
        val bob: Vertex = graph.addVertex("label", "Person", "name", "Bob")
        val knows: Edge = alice.addEdge("knows", bob)
        val g = GraphTraversalSource(graph)
        kotlin.test.assertEquals(2, g.V().hasLabel("Person").toList().size)
        kotlin.test.assertEquals(1, g.V().hasLabel("Person").has("name", "Alice").toList().size)
        kotlin.test.assertEquals(listOf("Bob", "Person"), g.V().hasLabel("Person").out("knows").values<String>("name").toList())
        kotlin.test.assertEquals(listOf("Alice", "Person"), g.V().hasLabel("Person").`in`("knows").values<String>("name").toList())
    }
}