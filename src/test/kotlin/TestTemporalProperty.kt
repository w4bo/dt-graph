import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Operators
import it.unibo.graph.query.Step
import it.unibo.graph.query.query
import it.unibo.graph.structure.CustomGraph
import it.unibo.graph.utils.EDGE
import kotlin.test.Test
import kotlin.test.assertEquals

class TestTemporalProperty {

    private fun init(): Graph {
        val g = CustomGraph(MemoryGraph())
        g.tsm = AsterixDBTSM.createDefault(g)
        g.clear()
        g.getTSM().clear()
        return g
    }

    private fun setup(): Graph {
        val g = init()
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
        assertEquals(
            setOf(listOf("Foo", "b1", "c1"), listOf("Bar", "b1", "c2")),
            query(g, listOf(Step("A", alias = "a"), null, Step("B", alias = "b"), null, Step("C", alias = "c")), by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"), Aggregate("c", property = "name"))).toSet()
        )

        // MATCH (a:A)-->(b:B)-->(c:C) RETURN a.name, b.name, c.lastname
        assertEquals(
            setOf(listOf("Foo", "b1", "null"), listOf("Bar", "b1", "c2.lastname")),
            query(g, listOf(Step("A", alias = "a"), null, Step("B", alias = "b"), null, Step("C", alias = "c")), by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"), Aggregate("c", property = "lastname"))).toSet()
        )
    }

    @Test
    fun test3() {
        val g = init()
        val a1 = g.addNode("A")
        val b1 = g.addNode("B")
        val e1 = g.addEdge("foo", a1.id, b1.id, from = 0, to = 2)
        g.addProperty(e1.id.toLong(), "name", "Foo", PropType.STRING, from = 0, to = 1, sourceType = EDGE)
        g.addProperty(e1.id.toLong(), "name", "Bar", PropType.STRING, from = 1, to = 2, sourceType = EDGE)

        val pattern = listOf(Step("A"), Step("foo", alias = "e"), Step("B"))
        val pattern1 = listOf(Step("A"), Step("foo", alias = "e", properties = listOf(Triple("name", Operators.EQ, "Foo"))), Step("B"))
        val pattern2 = listOf(Step("A"), Step("foo", alias = "e", properties = listOf(Triple("name", Operators.EQ, "Bar"))), Step("B"))

        // MATCH (a:A)-(e)->(b:B)-->(c:C) RETURN a, b, e
        assertEquals(1, query(g, pattern).size)
        // MATCH (a:A)-(e)->(b:B)-->(c:C) WHERE timestamp in [2, 3) RETURN a, b, e
        assertEquals(0, query(g, pattern, from = 2, to = 3).size)
        // MATCH (a:A)-(e {"name": "Foo"})->(b:B)-->(c:C) RETURN a, b, e
        assertEquals(1, query(g, pattern1).size)
        // MATCH (a:A)-(e {"name": "Bar"})->(b:B)-->(c:C) RETURN a, b, e
        assertEquals(1, query(g, pattern2).size)
        // MATCH (a:A)-(e {"name": "Bar"})->(b:B)-->(c:C) WHERE timestamp in [0, 1) RETURN a, b, e
        assertEquals(0, query(g, pattern2, from = 0, to = 1).size)
        // MATCH (a:A)-(e {"name": "Bar"})->(b:B)-->(c:C) WHERE timestamp in [1, 2) RETURN a, b, e
        assertEquals(1, query(g, pattern2, from = 1, to = 2).size)
        // MATCH (a:A)-(e {"name": "Foo"})->(b:B)-->(c:C) WHERE timestamp in [0, 1) RETURN a, b, e
        assertEquals(1, query(g, pattern1, from = 0, to = 1).size)
        // MATCH (a:A)-(e {"name": "Foo"})->(b:B)-->(c:C) WHERE timestamp in [1, 2) RETURN a, b, e
        assertEquals(0, query(g, pattern1, from = 1, to = 2).size)

        // MATCH (a:A)-(e)->(b:B)-->(c:C) RETURN e.name
        assertEquals(setOf("Foo", "Bar"), query(g, pattern, by = listOf(Aggregate("e", property = "name"))).toSet())
        // MATCH (a:A)-(e)->(b:B)-->(c:C) WHERE timestamp in [0, 1) RETURN e.name
        assertEquals(setOf("Foo"), query(g, pattern, by = listOf(Aggregate("e", property = "name")), from = 0, to = 1).toSet())
        // MATCH (a:A)-(e)->(b:B)-->(c:C) WHERE timestamp in [1, 2) RETURN e.name
        assertEquals(setOf("Bar"), query(g, pattern, by = listOf(Aggregate("e", property = "name")), from = 1, to = 2).toSet())
    }

    @Test
    fun test4() {
        val g = init()
        val a1 = g.addNode("A")
        val b1 = g.addNode("B")
        val e1 = g.addEdge("foo", a1.id, b1.id, from = 0, to = 2)
        g.addProperty(a1.id, "name", "A1", PropType.STRING)
        g.addProperty(b1.id, "name", "B1-Foo", PropType.STRING, from = 0, to = 1)
        g.addProperty(b1.id, "name", "B1-Bar", PropType.STRING, from = 1, to = 2)
        g.addProperty(e1.id.toLong(), "name", "E1-Foo", PropType.STRING, from = 0, to = 1, sourceType = EDGE)
        g.addProperty(e1.id.toLong(), "name", "E1-Bar", PropType.STRING, from = 1, to = 2, sourceType = EDGE)

        assertEquals(Long.MIN_VALUE, a1.getProps(name="name").first().fromTimestamp)
        assertEquals(Long.MAX_VALUE, a1.getProps(name="name").first().toTimestamp)
        assertEquals(1, a1.getProps(name = "name", fromTimestamp = 0, toTimestamp = 1).size)
        assertEquals(0, b1.getProps(name="name").minOf { it.fromTimestamp })
        assertEquals(2, b1.getProps(name="name").maxOf { it.toTimestamp })
        assertEquals(0, e1.getProps(name="name").minOf { it.fromTimestamp })
        assertEquals(2, e1.getProps(name="name").maxOf { it.toTimestamp })

        val pattern = listOf(Step("A", alias = "a"), Step("foo", alias = "e"), Step("B", alias = "b"))

        // MATCH (a:A)-(e)->(b:B)-->(c:C) RETURN a.name, e.name, b.name
        assertEquals(
            setOf(listOf("A1", "E1-Foo", "B1-Foo"), listOf("A1", "E1-Bar", "B1-Bar")),
            query(g, pattern,
                by = listOf(
                    Aggregate("a", property = "name"),
                    Aggregate("e", property = "name"),
                    Aggregate("b", property = "name")
                )
            ).toSet()
        )
    }

//    @Test
//    fun test5() {
//        val g = init()
//        val a1 = g.addNode("A")
//        val b1 = g.addNode("B")
//        g.addEdge("foo", a1.id, b1.id)
//        g.addEdge("foo", a1.id, b1.id, from = 0, to = 2)
//        g.addEdge("foo", a1.id, b1.id, from = 3, to = 4)
//        g.addEdge("foo", a1.id, b1.id, from = 4)
//
//        val pattern = listOf(Step("A", alias = "a"), Step("foo", alias = "e"), Step("B", alias = "b"))
//
//        // MATCH (a:A)-(e)->(b:B)-->(c:C) RETURN a, e, b
//        assertEquals(1, query(g, pattern).size)
//        // MATCH (a:A)-(e)->(b:B)-->(c:C) WHERE timestamp in [-1, 0) RETURN a, e, b
//        assertEquals(1, query(g, pattern, from = -1, to = 0).size)
//        // MATCH (a:A)-(e)->(b:B)-->(c:C) WHERE timestamp in [-1, inf) RETURN a, e, b
//        assertEquals(1, query(g, pattern, from = -1).size)
//        // MATCH (a:A)-(e)->(b:B)-->(c:C) WHERE timestamp in [1, 2) RETURN a, e, b
//        assertEquals(1, query(g, pattern, from = 1, to = 2).size)
//        // MATCH (a:A)-(e)->(b:B)-->(c:C) WHERE timestamp in [2, 3) RETURN a, e, b
//        assertEquals(0, query(g, pattern, from = 2, to = 3).size)
//        // MATCH (a:A)-(e)->(b:B)-->(c:C) WHERE timestamp in [4, inf) RETURN a, e, b
//        assertEquals(0, query(g, pattern, from = 4).size)
//    }
}