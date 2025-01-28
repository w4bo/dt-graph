enum class PropType { INT, DOUBLE, STRING, TS, GEOMETRY }

class P(val id: Int, val node: Int, val key: String, val value: Any, val type: PropType, var next: Int? = null) {
    init {
        val n = Graph.nodes[node]
        if (n.nextProp == null) n.nextProp = id
        else {
            next = n.nextProp
            n.nextProp = id
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $node, key: $key, value: $value}"
    }
}