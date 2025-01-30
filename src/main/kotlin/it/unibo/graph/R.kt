package it.unibo.graph

enum class Direction { IN, OUT }

open class R(val id: Int, val type: String, val fromN: Int, val toN: Int, var fromNextRel: Int? = null, var toNextRel: Int? = null) {
    init {
        val from = Graph.nodes[fromN]
        val to = Graph.nodes[toN]

        if (from.nextRel == null) {
            from.nextRel = id
        } else {
            fromNextRel = from.nextRel
            from.nextRel = id
        }

        if (to.nextRel == null) {
            to.nextRel = id
        } else {
            toNextRel = to.nextRel
            to.nextRel = id
        }
    }

    override fun toString(): String {
        return "($fromN)-[id: $id, type: $type}->($toN)"
    }
}