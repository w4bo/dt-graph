import it.unibo.graph.App
import it.unibo.graph.App.tsm
import it.unibo.graph.interfaces.*
import it.unibo.graph.query.Operators
import it.unibo.graph.query.Step
import it.unibo.graph.query.search
import org.junit.jupiter.api.Assertions.assertFalse
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue

class TestTemporal {

    val g = App.g

    @BeforeTest
    fun setup() {
        g.clear()
        tsm.clear()

        val f1 = g.addNode("AgriFarm")
        val p1 = g.addNode("AgriParcel")
        val p2 = g.addNode("AgriParcel")
        val d1 = g.addNode("Device")
        val d2 = g.addNode("Device")
        val h1 = g.addNode("Person", from = 0)
        val h2 = g.addNode("Person", from = 2)

        val r1 = g.addEdge("hasParcel", f1.id, p1.id)
        g.addProperty(r1.id.toLong(),"dateChanged", "today", PropType.STRING, from = 0, to = 1, sourceType = EDGE)
        g.addProperty(r1.id.toLong(),"dateChanged", "yesterday", PropType.STRING, from = 2, sourceType = EDGE)
        g.addEdge("hasParcel", f1.id, p2.id)
        g.addEdge("hasDevice", p1.id, d1.id, from = 0, to = 2)
        g.addEdge("hasDevice", p1.id, d2.id, from = 2, to = 4)
        g.addEdge("hasDevice", p2.id, d2.id, from = 0, to = 2)
        g.addEdge("hasDevice", p2.id, d1.id, from = 2, to = 4)
        g.addEdge("hasManutentor", d1.id, h1.id, from = 0, to = 2)
        g.addEdge("hasManutentor", d1.id, h2.id, from = 2, to = 4)
        g.addProperty(h1.id, "name", "Alice", PropType.STRING)
        g.addProperty(h1.id, "address", "Foo", PropType.STRING, from = 0, to = 1)
        g.addProperty(h1.id, "address", "Bar", PropType.STRING, from = 2)

        val ts1: TS = tsm.addTS()
        val m1 = ts1.add("Measurement", timestamp = 0, value = 0)
        Thread.sleep(1)
        ts1.add("Measurement", timestamp = 1, value = 1)
        Thread.sleep(1)
        ts1.add("Measurement", timestamp = 2, value = 2)
        val t1 = g.addNode("Temperature", value = ts1.getTSId())
        g.addEdge("hasTemperature", d1.id, t1.id)
        g.addEdge("hasOwner", m1.id, h1.id, from = 0, to = 2, id = DUMMY_ID)
        g.addEdge("hasOwner", m1.id, h2.id, from = 2, to = 4, id = DUMMY_ID)

        val ts2 = tsm.addTS()
        ts2.add("Measurement", timestamp = 0, value = 10)
        Thread.sleep(1)
        ts2.add("Measurement", timestamp = 1, value = 11)
        Thread.sleep(1)
        ts2.add("Measurement", timestamp = 2, value = 12)
        val t2 = g.addNode("Temperature", value = ts2.getTSId())
        g.addEdge("hasTemperature", d2.id, t2.id)
    }

    @Test
    fun testSearch0() {
        kotlin.test.assertEquals(
            6,
            search(
                listOf(
                    Step("Device"),
                    Step("hasTemperature"),
                    Step("Temperature"),
                    Step(HAS_TS),
                    Step("Measurement")
                ), timeaware = true
            ).size
        )
    }

    @Test
    fun testSearch1() {
        kotlin.test.assertEquals(
            4,
            search(
                listOf(
                    Step("Device"),
                    Step("hasTemperature"),
                    Step("Temperature"),
                    Step(HAS_TS),
                    Step("Measurement")
                ), timeaware = true, from = 0, to = 1
            ).size
        )
    }

    @Test
    fun testSearch2() {
        kotlin.test.assertEquals(
            1,
            search(
                listOf(
                    Step("Device"),
                    Step("hasManutentor"),
                    Step("Person"),
                ), timeaware = true, from = 0, to = 1
            ).size
        )
    }

    @Test
    fun testSearch3() {
        kotlin.test.assertEquals(
            1,
            search(
                listOf(
                    Step("Person"),
                ), timeaware = true, from = 0, to = 1
            ).size
        )
    }

    @Test
    fun testSearch4() {
        val steps = listOf(
            Step("Device"),
            Step("hasTemperature"),
            Step("Temperature"),
            Step(HAS_TS),
            Step("Measurement"),
            Step("hasOwner"),
            Step("Person")
        )
        kotlin.test.assertEquals(1, search(steps, timeaware = true).size)
        kotlin.test.assertEquals(1, search(steps, timeaware = true, from = 0, to = 4).size)
        kotlin.test.assertEquals(1, search(steps, timeaware = true, from = 0, to = 0).size)
        kotlin.test.assertEquals(0, search(steps, timeaware = true, from = 1, to = 1).size)
        kotlin.test.assertEquals(2, search(steps, timeaware = false).size)
    }

    @Test
    fun testSearch5() {
        val steps = listOf(
            Step("Device"),
            Step("hasTemperature"),
            Step("Temperature"),
            Step(HAS_TS),
            Step("Measurement"),
            Step("hasOwner"),
            Step("Person", listOf(Triple("name", Operators.EQ, "Alice")))
        )
        kotlin.test.assertEquals(1, search(steps, timeaware = true).size)
        kotlin.test.assertEquals(1, search(steps, timeaware = false).size)
    }

    @Test
    fun testSearch6() {
        val steps = listOf(
            Step("Person", listOf(Triple("name", Operators.EQ, "Alice"), Triple("address", Operators.EQ, "Foo")))
        )
        kotlin.test.assertEquals(1, search(steps, timeaware = true, from = 0, to = 0).size)
        kotlin.test.assertEquals(1, search(steps, timeaware = false).size)
        kotlin.test.assertEquals(0, search(steps, timeaware = true, from = 2).size)
        kotlin.test.assertEquals(0, search(steps, timeaware = true, to = -1).size)
    }

    @Test
    fun testSearch7() {
        var steps = listOf(Step("AgriFarm"), Step("hasParcel"), Step("AgriParcel"))
        kotlin.test.assertEquals(2, search(steps, timeaware = false).size)

        steps = listOf(Step("AgriFarm"), Step("hasParcel", listOf(Triple("dateChanged", Operators.EQ, "today"))), Step("AgriParcel"))
        kotlin.test.assertEquals(1, search(steps, timeaware = false).size)
        kotlin.test.assertEquals(1, search(steps, timeaware = true, from = 0, to = 0).size)
        kotlin.test.assertEquals(0, search(steps, timeaware = true, from = 2).size)
    }

    @Test
    fun testTimeOverlap() {
        assertTrue(timeOverlap(0, 0, 0, 1)) // [0, 0) and [0, 1)
        assertTrue(timeOverlap(0, 2, 0, 1)) // [0, 2) and [0, 1)
        assertTrue(timeOverlap(0, 1, 0, 1)) // [0, 1) and [0, 1)
        assertTrue(timeOverlap(0, 0, 0, 0)) // [0, 0) and [0, 0)
        assertFalse(timeOverlap(0, 0, 1, 2)) // [0, 0) and [1, 2)
        assertFalse(timeOverlap(1, 2, 0, 0)) // [1, 2) and [0, 0)
        assertFalse(timeOverlap(0, 1, 1, 2)) // [0, 1) and [1, 2)
        assertTrue(timeOverlap(0, 0, 0, 1)) // [0, 0) and [0, 1)
    }
}