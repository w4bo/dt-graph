import org.junit.Before
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

        val n4 = N(Graph.nodes.size, "Device")
        Graph.nodes += n4

        Graph.rels += R(Graph.rels.size, "hasParcel", n1!!.id, n2!!.id)
        Graph.rels += R(Graph.rels.size, "hasDevice", n1!!.id, n3!!.id)
        Graph.rels += R(Graph.rels.size, "hasDevice", n2!!.id, n3!!.id)
        Graph.rels += R(Graph.rels.size, "hasDevice", n2!!.id, n4.id)

        val ts1 = TS(Graph.ts.size)
        Graph.ts += ts1
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), "humidity", 10))
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), "humidity", 11))
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), "humidity", 12))
        Graph.props += P(Graph.props.size, n1!!.id, PropType.TS.toString(), ts1.id, PropType.TS)

        val ts2 = TS(Graph.ts.size)
        Graph.ts += ts2
        ts2.add(TSelem(ts2.values.size, ts2.id, System.currentTimeMillis(), "temperature", 10))
        ts2.add(TSelem(ts2.values.size, ts2.id, System.currentTimeMillis(), "temperature", 11))
        ts2.add(TSelem(ts2.values.size, ts2.id, System.currentTimeMillis(), "temperature", 12))
        Graph.props += P(Graph.props.size, n2!!.id, PropType.TS.toString(), ts2.id, PropType.TS)

        // val ts3 = TS(Graph.ts.size)
        // Graph.ts += ts3
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), "solarRadiation", 13))
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), "solarRadiation", 14))
        ts1.add(TSelem(ts1.values.size, ts1.id, System.currentTimeMillis(), "solarRadiation", 15))
        // Graph.props += P(Graph.props.size, n1.id, PropType.TS.toString(), ts3.id, PropType.TS)
    }

    @Test
    fun testSum() {
        assertEquals(42, 40 + 2)
    }

    @Test
    fun testProps() {
        assertEquals(n1!!.getProps().size, 3)
        n1!!.getProps().forEach({ println(it.toString()) })
        assertEquals(n1!!.getRels().size, 2)
        n1!!.getRels().forEach({ println(it.toString()) })
        assertEquals(n2!!.getRels().size, 2, n2!!.getRels().toString())
        assertEquals(n1!!.getTS().size, 6)
        n1!!.getTS().forEach({ println(it.toString()) })
        assertEquals(n2!!.getTS().size, 3)
        n2!!.getTS().forEach({ println(it.toString()) })
    }

    fun pattern2string(patterns: List<List<N>>): String {
        return patterns.map { pattern -> pattern.map { node -> node.toString() }.joinToString(", ") }.joinToString("\n")
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
}