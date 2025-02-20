package it.unibo.graph

const val DUMMY_ID = -1
const val NODE = true
const val EDGE = false

enum class PropType { INT, DOUBLE, STRING, TS, GEOMETRY }

open class P(
    final override val id: Int,
    val sourceId: Long,
    val sourceType: Boolean,
    val key: String,
    val value: Any,
    val type: PropType,
    var next: Int? = null,
    override val fromTimestamp: Long = Long.MIN_VALUE,
    override var toTimestamp: Long = Long.MAX_VALUE
) : Elem {
    init {
        if (DUMMY_ID != id) {
            val n: ElemP = if (sourceType == NODE) App.g.getNode(sourceId) else App.g.getEdge(sourceId.toInt())
            if (n.nextProp == null) n.nextProp = id
            else {
                next = n.nextProp
                n.nextProp = id
            }
            if (sourceType == NODE) App.g.addNode(n as N) else App.g.addEdge(n as R)
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $sourceId, key: $key, value: $value, type: $type}"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is P) return false
        return id == other.id
    }
}