package it.unibo.graph

open class R(val id: Int, val type: String, val fromN: Int, val toN: Int, var fromNextRel: Int? = null, var toNextRel: Int? = null) {
    init {
        val from = Graph.nodes[fromN] // Graph.nodes[nextNode]
        val to = Graph.nodes[toN] // Graph.nodes[nextNode]

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