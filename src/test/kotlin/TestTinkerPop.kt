import it.unibo.graph.structure.CustomGraph
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Vertex
import kotlin.test.BeforeTest
import kotlin.test.Test


class TestTinkerPop {

    val graph = CustomGraph()
    val g = GraphTraversalSource(graph)

    @BeforeTest
    fun setup() {
        graph.close()

        val n1: Vertex = graph.addVertex("label", "AgriFarm", "name", "Errano", "location", "GeoJSON")
        val n2: Vertex = graph.addVertex("label", "AgriParcel", "name", "T0", "location", "GeoJSON")
        val n3: Vertex = graph.addVertex("label", "Device", "name", "GB1", "location", "GeoJSON")
        val n4: Vertex = graph.addVertex("label", "Device", "name", "GB2", "location", "GeoJSON")
        val n5: Vertex = graph.addVertex("label", "Person", "name", "Alice")
        val n6: Vertex = graph.addVertex("label", "Person", "name", "Bob")

        n1.addEdge("hasParcel", n2)
        n1.addEdge("hasDevice", n3)
        n2.addEdge("foo", n2)
        n2.addEdge("hasDevice", n3)
        n2.addEdge("hasDevice", n4)
        n3.addEdge("hasOwner", n5)
        n4.addEdge("hasOwner", n6)
        n3.addEdge("hasManutentor", n5)
        n4.addEdge("hasManutentor", n6)
    }

    @Test
    fun test1() {
        kotlin.test.assertEquals(
            setOf("GB1", "GB2"),
            g.V()
                .hasLabel("AgriParcel")
                .out("hasDevice")
                .values<String>("name")
                .toSet()
        )
    }

    @Test
    fun test2() {
        kotlin.test.assertEquals(
            setOf("GB1", "GB2"),
            g.V()
                .hasLabel("AgriFarm")
                .out("hasParcel")
                .out("hasDevice")
                .values<String>("name")
                .toSet()
        )
    }
}