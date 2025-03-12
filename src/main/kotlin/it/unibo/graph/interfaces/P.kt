package it.unibo.graph.interfaces

import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.GRAPH_SOURCE
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.decodeBitwiseSource

enum class PropType { INT, DOUBLE, STRING, TS, GEOMETRY }

open class P(
    final override val id: Int,
    val sourceId: Long,
    val sourceType: Boolean,
    val key: String,
    val value: Any,
    val type: PropType,
    var next: Int? = null,
    final override val fromTimestamp: Long = Long.MIN_VALUE,
    final override var toTimestamp: Long = Long.MAX_VALUE,
    @Transient final override var g: Graph
) : Elem {
    init {
        if (decodeBitwiseSource(sourceId) == GRAPH_SOURCE && id != DUMMY_ID) {
            val n: ElemP = if (sourceType == NODE) g.getNode(sourceId) else g.getEdge(sourceId.toInt())
            if (n.nextProp == null) n.nextProp = id
            else {
                next = n.nextProp
                n.nextProp = id
            }
            if (sourceType == NODE) g.addNode(n as N) else g.addEdge(n as R)
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $sourceId, key: $key, value: $value, type: $type, from: $fromTimestamp, to: $toTimestamp}"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is P) return false
        return id == other.id
    }
}