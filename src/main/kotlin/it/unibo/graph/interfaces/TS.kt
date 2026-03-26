package it.unibo.graph.interfaces

import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.LOCATION
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.encodeBitwise
import org.locationtech.jts.io.WKTReader
import java.io.Serializable

interface TS : Serializable {
    val g: Graph

    fun getTSId(): Long

    fun createNode(label: String, timestamp: Long, value: Long): N = N(encodeBitwise(getTSId(), timestamp), label, value = value, fromTimestamp = timestamp, toTimestamp = timestamp, g = g)

    fun add(label: String, timestamp: Long, value: Long, location: String, isUpdate: Boolean = false, flush: Boolean = true): N {
        val n = createNode(label, timestamp, value)
        n.properties.add(P(DUMMY_ID, n.id, NODE, LOCATION, WKTReader().read(location), PropType.GEOMETRY, fromTimestamp = timestamp, toTimestamp = timestamp, g = g))
        return add(n, isUpdate, flush)
    }

    fun add(label: String, timestamp: Long, value: Long, isUpdate: Boolean = false, flush: Boolean = true) = add(createNode(label, timestamp, value), isUpdate, flush)

    fun add(n: N, isUpdate: Boolean, flush: Boolean = true): N
    fun getValues(by: List<Aggregate>, filters: List<Filter>, isGroupBy: Boolean = false): List<N>
    fun get(id: Long): N
}