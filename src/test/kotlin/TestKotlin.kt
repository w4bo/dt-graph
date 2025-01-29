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

        val n4 = N(Graph.nodes.size, "Device")
        Graph.nodes += n4

        n1 = N(Graph.nodes.size, "AgriFarm")
        Graph.nodes += n1!!
        Graph.props += P(Graph.props.size, n1!!.id, "name", "Errano", PropType.STRING)
        Graph.props += P(Graph.props.size, n1!!.id, "location", "GeoJSON", PropType.GEOMETRY)

        n2 = N(Graph.nodes.size, "AgriParcel")
        Graph.nodes += n2!!
        Graph.props += P(Graph.props.size, n2!!.id, "name", "T0", PropType.STRING)
        Graph.props += P(Graph.props.size, n2!!.id, "location", "GeoJSON", PropType.GEOMETRY)

        n3 = N(Graph.nodes.size, "Device")
        Graph.nodes += n3!!
        Graph.props += P(Graph.props.size, n3!!.id, "name", "GB", PropType.STRING)
        Graph.props += P(Graph.props.size, n3!!.id, "location", "GeoJSON", PropType.GEOMETRY)

        val n5 = N(Graph.nodes.size, "Person")
        Graph.nodes += n5

        val n6 = N(Graph.nodes.size, "Person")
        Graph.nodes += n6

        Graph.rels += R(Graph.rels.size, "hasParcel", n1!!.id, n2!!.id)
        Graph.rels += R(Graph.rels.size, "hasDevice", n1!!.id, n3!!.id)
        Graph.rels += R(Graph.rels.size, "hasDevice", n2!!.id, n3!!.id)
        Graph.rels += R(Graph.rels.size, "foo", n2!!.id, n2!!.id)
        Graph.rels += R(Graph.rels.size, "hasDevice", n2!!.id, n4.id)
        Graph.rels += R(Graph.rels.size, "hasOwner", n3!!.id, n5.id)
        Graph.rels += R(Graph.rels.size, "hasOwner", n4.id, n5.id)
        Graph.rels += R(Graph.rels.size, "hasManutentor", n3!!.id, n6.id)
        Graph.rels += R(Graph.rels.size, "hasManutentor", n4.id, n6.id)

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
        Graph.props += P(Graph.props.size, n2!!.id, "temperature",  ts2.id, PropType.TS)

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
        listOf(Pair(n1!!, 2), Pair(n2!!, 3), Pair(n3!!, 2)).forEach {
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
}