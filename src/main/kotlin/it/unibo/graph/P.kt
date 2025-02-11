package it.unibo.graph

import java.io.Serializable

val DUMMY_ID = -1

enum class PropType { INT, DOUBLE, STRING, TS, GEOMETRY }

open class P(val id: Int, val node: Int, val key: String, val value: Any, val type: PropType, var next: Int? = null): Serializable {
    init {
        if (DUMMY_ID != id) {
            val n = App.g.getNode(node)
            if (n.nextProp == null) n.nextProp = id
            else {
                next = n.nextProp
                n.nextProp = id
            }
            App.g.addNode(n)
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $node, key: $key, value: $value, type: $type}"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is P) return false
        return id == other.id
    }
}