package it.unibo.graph

import org.apache.commons.lang3.NotImplementedException
import java.io.Serializable

val HAS_TS = "hasTS"
val VALUE = "value"
val GRAPH_SOURCE = 0L

open class N (
    val id: Long, // node id
    val type: String, // node label
    var nextRel: Int? = null, // if graph node, link to the next relationship
    var nextProp: Int? = null, // if graph node, link to the next property
    val value: Long? = null, // if TS snapshot: value of the measurement, else if TS node: id of the TS
    val timestamp: Long? = null, // if TS snapshot: timestamp of the measurement
    val location: Pair<Double, Double>? = null, // location
    val relationships: MutableList<R> = mutableListOf() // if TS snapshot, lists of relationships towards the graph
): Serializable {

    fun getProps(next: Int? = nextProp, filter: PropType? = null, name: String? = null): List<P> {
        if (value != null && name == VALUE) return listOf(P(DUMMY_ID, id, VALUE, value, PropType.DOUBLE))
        return if (next == null) {
            emptyList()
        } else {
            val p = App.g.getProp(next)
            (if ((filter == null || p.type == filter) && (name == null || p.key == name)) listOf(p) else emptyList()) + getProps(
                p.next,
                filter,
                name
            )
        }
    }

    fun getTS(): List<N> {
        return App.tsm.getTS(value!!).getValues()
    }

    fun getRels(next: Int? = nextRel, direction: Direction? = null, label: String? = null): List<R> {
        return if (label == HAS_TS) { // Jump to the time series
            listOf(R(DUMMY_ID, HAS_TS, id, value!!))
        } else { // Iterate within the graph
            if (timestamp != null) { // If TS snapshot
                when (direction) {
                    Direction.IN -> throw NotImplementedException()
                    else -> relationships.filter { label == null || it.type == label }.toList()
                }
            } else { // Graph node
                if (next == null) return emptyList()
                val r = App.g.getEdge(next)
                if (label == null || r.type == label) {
                    when (direction) {
                        Direction.IN -> if (r.toN == id) listOf(r) else emptyList()
                        Direction.OUT -> if (r.fromN == id) listOf(r) else emptyList()
                        else -> listOf(r)
                    }
                } else {
                    emptyList()
                } + getRels(if (r.fromN == id) r.fromNextRel else r.toNextRel, direction, label)
            }
        }
    }

    override fun toString(): String {
        return "(id: $id, type: $type)"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is N) return false
        return id == other.id && type == other.type
    }
}