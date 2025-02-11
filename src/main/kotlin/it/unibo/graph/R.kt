package it.unibo.graph

import java.io.Serializable

enum class Direction { IN, OUT }

open class R(val id: Int, val type: String, val fromN: Int, val toN: Int, var fromNextRel: Int? = null, var toNextRel: Int? = null):
    Serializable {
    init {
        if (id != DUMMY_ID) {
            val from = App.g.getNode(fromN)
            val to = App.g.getNode(toN)

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

            App.g.addNode(from)
            App.g.addNode(to)
        }
    }

    override fun toString(): String {
        return "($fromN)-[id: $id, type: $type}->($toN)"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is R) return false
        return id == other.id
    }
}