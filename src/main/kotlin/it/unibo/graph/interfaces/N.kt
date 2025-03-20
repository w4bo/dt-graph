package it.unibo.graph.interfaces

import it.unibo.graph.interfaces.Labels.HasTS
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.VALUE
import org.apache.commons.lang3.NotImplementedException

open class N(
    final override val id: Long, // node id
    final override val type: Label, // node label
    var nextRel: Int? = null, // if graph node, link to the next relationship
    final override var nextProp: Int? = null, // if graph node, link to the next property
    val value: Long? = null, // if TS snapshot: value of the measurement, else if TS node: pointer to the id of the TS
    val timestamp: Long? = null, // if TS snapshot: timestamp of the measurement
    @Transient val relationships: MutableList<R> = mutableListOf(), // if TS snapshot, lists of relationships towards the graph
    @Transient final override val properties: MutableList<P> = mutableListOf(), // if TS snapshot, lists of properties
    final override val fromTimestamp: Long = Long.MIN_VALUE,
    final override var toTimestamp: Long = Long.MAX_VALUE,
    @Transient final override var g: Graph
) : ElemP {

    override fun getProps(next: Int?, filter: PropType?, name: String?, fromTimestamp: Long, toTimestamp: Long, timeaware: Boolean): List<P> {
        return when (name) {
            VALUE -> if (value != null) listOf(P(DUMMY_ID, id, NODE, VALUE, value, PropType.DOUBLE, g = g)) else emptyList()
            else -> {
                super.getProps(next, filter, name, fromTimestamp, toTimestamp, timeaware)
            }
        }
    }

    fun getTS(): List<N> {
        return g.getTSM().getTS(value!!).getValues()
    }

    fun getRels(next: Int? = nextRel, direction: Direction? = null, label: Label? = null, includeHasTs: Boolean = false): List<R> {
        return if (label == HasTS) { // Jump to the time series
            listOf(R(DUMMY_ID, HasTS, id, value!!, g = g))
        } else { // Iterate within the graph
            if (timestamp != null) { // If TS snapshot
                when (direction) {
                    Direction.IN -> throw NotImplementedException()
                    else -> relationships.filter { label == null || it.type == label }.toList()
                }
            } else { // Graph node
                if (next == null) return if(includeHasTs && value != null) listOf(R(DUMMY_ID, HasTS, id, value, g = g)) else emptyList()
                val r = g.getEdge(next)
                if (label == null || r.type == label) {
                    when (direction) {
                        Direction.IN -> if (r.toN == id) listOf(r) else emptyList()
                        Direction.OUT -> if (r.fromN == id) listOf(r) else emptyList()
                        else -> listOf(r)
                    }
                } else {
                    emptyList()
                } + getRels(if (r.fromN == id) r.fromNextRel else r.toNextRel, direction, label, includeHasTs)
            }
        }
    }

    override fun toString(): String {
        return "(id: $id, type: $type, from: $fromTimestamp, to: $toTimestamp)"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is N) return false
        return id == other.id && type == other.type
    }
}