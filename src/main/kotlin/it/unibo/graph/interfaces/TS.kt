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
    fun add(label: Label, timestamp: Long, value: Long, location: String?): N {
        val nodeId = encodeBitwise(getTSId(), timestamp)
        val newTSNode = N(nodeId, label, value = value, fromTimestamp = timestamp, toTimestamp = timestamp, g = g)
        newTSNode.properties.add(P(DUMMY_ID, nodeId, NODE, LOCATION, location!!, PropType.GEOMETRY, g = g))
        return add(newTSNode)
    }

    fun add(label: Label, timestamp: Long, value: Long) = add(label, timestamp, value, null)

    fun add(n: N): N
    fun getValues(by: List<Aggregate>, filter: List<Filter>): List<N>
    fun get(id: Long): N
}