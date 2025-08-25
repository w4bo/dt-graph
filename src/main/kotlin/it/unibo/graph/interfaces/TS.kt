package it.unibo.graph.interfaces

import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.LOCATION
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.*
import org.locationtech.jts.io.WKTReader
import java.io.Serializable

interface TS : Serializable {
    val g: Graph

    fun getTSId(): Long

    fun createNode(label: Label, timestamp: Long, value: Long): N = N(encodeBitwise(getTSId(), timestamp), label, value = value, fromTimestamp = timestamp, toTimestamp = timestamp, g = g)

    fun add(label: Label, timestamp: Long, value: Long, location: String, isUpdate: Boolean = false): N {
        val n = createNode(label, timestamp, value)
        n.properties.add(P(DUMMY_ID, n.id, NODE, LOCATION, WKTReader().read(location), PropType.GEOMETRY, fromTimestamp = timestamp, toTimestamp = timestamp, g = g))
        return add(n, isUpdate)
    }

    fun add(label: Label, timestamp: Long, value: Long, isUpdate: Boolean = false) = add(createNode(label, timestamp, value), isUpdate)

    fun add(n: N, isUpdate: Boolean): N
    fun getValues(by: List<Aggregate>, filters: List<Filter>, isGroupBy: Boolean = false): List<N>
    fun get(id: Long): N
}