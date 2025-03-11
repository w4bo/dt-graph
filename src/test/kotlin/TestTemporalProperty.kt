import it.unibo.graph.*
import it.unibo.graph.App.tsm
import kotlin.test.BeforeTest
import kotlin.test.Test

class TestTemporalProperty {
    val g = App.g

    @BeforeTest
    fun setup() {
        g.clear()
        tsm.clear()

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
    }

    @Test
    fun test1() {
        // MATCH (a:A)-->(b:B) RETURN a.name, b.name
        kotlin.test.assertEquals(
            setOf(listOf("Foo", "b1"), listOf("Bar", "b1"), listOf("Bar", "b2"), listOf("null", "b3")),
            query(listOf(Step("A", alias = "a"), null, Step("B", alias = "b")), by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"))).toSet()
        )
    }

    @Test
    fun test2() {
        // MATCH (a:A)-->(b:B)-->(c:C) RETURN a.name, b.name, c.name
        kotlin.test.assertEquals(
            setOf(listOf("Foo", "b1", "c1"), listOf("Bar", "b1", "c2")),
            query(listOf(Step("A", alias = "a"), null, Step("B", alias = "b"), null, Step("C", alias = "c")), by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"), Aggregate("c", property = "name"))).toSet()
        )

        // MATCH (a:A)-->(b:B)-->(c:C) RETURN a.name, b.name, c.lastname
        kotlin.test.assertEquals(
            setOf(listOf("Foo", "b1", "null"), listOf("Bar", "b1", "c2.lastname")),
            query(listOf(Step("A", alias = "a"), null, Step("B", alias = "b"), null, Step("C", alias = "c")), by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"), Aggregate("c", property = "lastname"))).toSet()
        )
    }
}