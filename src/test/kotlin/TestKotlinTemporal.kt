import it.unibo.graph.*
import it.unibo.graph.App.tsm
import it.unibo.graph.strategy.TrackEdgeWeightStrategy
import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue


class TestKotlinTemporal {

    val g = App.g

    @BeforeTest
    fun setup() {
        g.clear()
        tsm.clear()

        val n1 = g.addNode("AgriFarm")
        val n2 = g.addNode("AgriParcel")
        val n3 = g.addNode("AgriParcel")
        val n4 = g.addNode("Device")
        val n5 = g.addNode("Device")

        g.addEdge("hasParcel", n1.id, n2.id)
        g.addEdge("hasParcel", n1.id, n3.id)
        g.addEdge("hasDevice", n2.id, n4.id, from = 0, to = 2)
        g.addEdge("hasDevice", n2.id, n5.id, from = 2, to = 4)
        g.addEdge("hasDevice", n3.id, n4.id, from = 2, to = 4)
        g.addEdge("hasDevice", n3.id, n5.id, from = 0, to = 2)
    }

    @Test
    fun testTemporalTraverse1() {
        val g = GraphTraversalSource(g).withStrategies(TrackEdgeWeightStrategy())

        kotlin.test.assertEquals(
            4, g.V()
                .hasLabel("AgriFarm")
                .outE("hasParcel")
                .outE("hasDevice")
                .toList().size
        )
    }
}