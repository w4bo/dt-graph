package it.unibo.graph

open class R(val id: Int, val type: String, val prevNode: Int, val nextNode: Int, var next: Int? = null) {
    init {
        listOf(Graph.nodes[prevNode]).forEach { // , Graph.nodes[nextNode]
            if (it.nextRel == null) {
                it.nextRel = id
            } else {
                next = it.nextRel
                it.nextRel = id
            }
        }
    }

    override fun toString(): String {
        return "($prevNode)-[id: $id, type: $type}->($nextNode)"
    }
}