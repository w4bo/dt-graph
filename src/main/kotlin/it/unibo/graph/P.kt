package it.unibo.graph

val DUMMY_ID = -1

enum class PropType { INT, DOUBLE, STRING, TS, GEOMETRY }

open class P(
    final override val id: Int,
    val nodeId: Long,
    val key: String,
    val value: Any,
    val type: PropType,
    var next: Int? = null,
    override val fromTimestamp: Long = Long.MIN_VALUE,
    override var toTimestamp: Long = Long.MAX_VALUE
) : Elem {
    init {
        if (DUMMY_ID != id) {
            val n = App.g.getNode(nodeId)
            if (n.nextProp == null) n.nextProp = id
            else {
                next = n.nextProp
                n.nextProp = id
            }
            App.g.addNode(n)
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $nodeId, key: $key, value: $value, type: $type}"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is P) return false
        return id == other.id
    }
}