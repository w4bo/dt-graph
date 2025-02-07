package it.unibo.graph

import java.io.Serializable

enum class PropType { INT, DOUBLE, STRING, TS, GEOMETRY }

open class P(val id: Int, val node: Int, val key: String, val value: Any, val type: PropType, var next: Int? = null):
    Serializable {
    init {
        val n = App.g.getNode(node)
        if (n.nextProp == null) n.nextProp = id
        else {
            next = n.nextProp
            n.nextProp = id
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $node, key: $key, value: $value, type: $type}"
    }
}