import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Step
import it.unibo.graph.query.query
import it.unibo.graph.structure.CustomGraph
import kotlin.test.Test

class TestTemporalProperty {

    fun setup(): Graph {
        val g = CustomGraph(MemoryGraph())
        g.tsm = AsterixDBTSM.createDefault(g)
        g.clear()
        g.getTSM().clear()

        val a = g.addNode("A")
        val b1 = g.addNode("B")
        val b2 = g.addNode("B")
        val b3 = g.addNode("B")
        val c1 = g.addNode("C")
        val c2 = g.addNode("C")

        g.addEdge("foo", a.id, b1.id, from = 0, to = 2)
        g.addEdge("foo", a.id, b2.id, from = 1, to = 1)
        g.addEdge("foo", a.id, b3.id, from = 2, to = 2)
        g.addEdge("foo", b1.id, c1.id, from = 0, to = 0)
        g.addEdge("foo", b1.id, c2.id, from = 1)

        g.addProperty(a.id,"name", "Foo", PropType.STRING, from = 0, to = 0)
        g.addProperty(a.id,"name", "Bar", PropType.STRING, from = 1, to = 1)
        g.addProperty(b1.id,"name", "b1", PropType.STRING)
        g.addProperty(b2.id,"name", "b2", PropType.STRING)
        g.addProperty(b3.id,"name", "b3", PropType.STRING)
        g.addProperty(c1.id,"name", "c1", PropType.STRING)
        g.addProperty(c1.id,"lastname", "c1.lastname", PropType.STRING, from = 1)
        g.addProperty(c2.id,"name", "c2", PropType.STRING)
        g.addProperty(c2.id,"lastname", "c2.lastname", PropType.STRING, from = 1)

        return g
    }

    @Test
    fun test1() {
        val g = setup()
        // MATCH (a:A)-->(b:B) RETURN a.name, b.name
        kotlin.test.assertEquals(
            setOf(listOf("Foo", "b1"), listOf("Bar", "b1"), listOf("Bar", "b2"), listOf("null", "b3")),
            query(g, listOf(Step("A", alias = "a"), null, Step("B", alias = "b")), by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"))).toSet()
        )
    }

    @Test
    fun test2() {
        val g = setup()
        // MATCH (a:A)-->(b:B)-->(c:C) RETURN a.name, b.name, c.name
        kotlin.test.assertEquals(
            setOf(listOf("Foo", "b1", "c1"), listOf("Bar", "b1", "c2")),
            query(g, listOf(Step("A", alias = "a"), null, Step("B", alias = "b"), null, Step("C", alias = "c")), by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"), Aggregate("c", property = "name"))).toSet()
        )

        // MATCH (a:A)-->(b:B)-->(c:C) RETURN a.name, b.name, c.lastname
        kotlin.test.assertEquals(
            setOf(listOf("Foo", "b1", "null"), listOf("Bar", "b1", "c2.lastname")),
            query(g, listOf(Step("A", alias = "a"), null, Step("B", alias = "b"), null, Step("C", alias = "c")), by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"), Aggregate("c", property = "lastname"))).toSet()
        )
    }
}