import it.unibo.graph.App
import it.unibo.graph.App.tsm
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.query.*
import kotlin.test.BeforeTest
import kotlin.test.Test

class TestTemporalJoin {
    val g = App.g

    @BeforeTest
    fun setup() {
        g.clear()
        tsm.clear()

        val a1 = g.addNode("A")
//        val a2 = g.addNode("A")
//        val b1 = g.addNode("B")
        val b2 = g.addNode("B")
//        val b3 = g.addNode("B")
        // val c1 = g.addNode("C")
        // val c2 = g.addNode("C")

        g.addProperty(a1.id,"name", "Foo", PropType.STRING, from = 0, to = 1)
        g.addProperty(a1.id,"name", "Bar", PropType.STRING, from = 1, to = 2)
//        g.addProperty(b1.id,"name", "Foo", PropType.STRING, from = 0, to = 1)
//        g.addProperty(b1.id,"name", "Bar", PropType.STRING, from = 1, to = 2)
        g.addProperty(b2.id,"name", "Bar", PropType.STRING, from = 1, to = 2)
    }

    @Test
    fun test1() {
        // MATCH (a:A)-->(b:B) WHERE a.name = b.name RETURN a.name, b.name
        kotlin.test.assertEquals(
            setOf(listOf("Bar", "Bar")),
            query(
                listOf(
                    listOf(Step("A", alias = "a")),
                    listOf(Step("B", alias = "b"))
                ),
                where = listOf(Compare("a", "b", property = "name", operator = Operators.EQ)),
                by = listOf(Aggregate("a", property = "name"), Aggregate("b", property = "name"))
            ).toSet()
        )
    }
}