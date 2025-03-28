package it.unibo.graph.interfaces

import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.LOCATION
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.encodeBitwise
import java.io.Serializable

interface TS : Serializable {
    val g: Graph

    fun getTSId(): Long

    fun createNode(label: Label, timestamp: Long, value: Long): N = N(encodeBitwise(getTSId(), timestamp), label, value = value, fromTimestamp = timestamp, toTimestamp = timestamp, g = g)

    fun add(label: Label, timestamp: Long, value: Long, location: String): N {
        val n = createNode(label, timestamp, value)
        n.properties.add(P(DUMMY_ID, n.id, NODE, LOCATION, location, PropType.GEOMETRY, fromTimestamp = timestamp, toTimestamp = timestamp, g = g))
        return add(n)
    }

    fun add(label: Label, timestamp: Long, value: Long) = add(createNode(label, timestamp, value))

    fun add(n: N): N
    fun getValues(by: List<Aggregate>, filters: List<Filter>): List<N>
    fun get(id: Long): N
}