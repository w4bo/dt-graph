import it.unibo.graph.*
import it.unibo.graph.App.tsm
import kotlin.test.BeforeTest
import kotlin.test.Test


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
        val h1 = g.addNode("Person")
        val h2 = g.addNode("Person")

        g.addEdge("hasParcel", f1.id, p1.id)
        g.addEdge("hasParcel", f1.id, p2.id)
        g.addEdge("hasDevice", p1.id, d1.id, from = 0, to = 2)
        g.addEdge("hasDevice", p1.id, d2.id, from = 2, to = 4)
        g.addEdge("hasDevice", p2.id, d2.id, from = 0, to = 2)
        g.addEdge("hasDevice", p2.id, d1.id, from = 2, to = 4)
        g.addEdge("hasManutentor", d1.id, h1.id, from = 0, to = 2)
        g.addEdge("hasManutentor", d1.id, h2.id, from = 2, to = 4)

        val ts1: TS = tsm.addTS()
        val m1 = ts1.add("Measurement", timestamp = 0, value = 0)
        Thread.sleep(1)
        ts1.add("Measurement", timestamp = 1, value = 1)
        Thread.sleep(1)
        ts1.add("Measurement", timestamp = 2, value = 2)
        val t1 = g.addNode("Temperature", value = ts1.getTSId())
        g.addEdge("hasTemperature", d1.id, t1.id)

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
            ).size)
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
}