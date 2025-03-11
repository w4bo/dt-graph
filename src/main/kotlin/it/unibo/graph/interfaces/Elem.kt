package it.unibo.graph.interfaces

import java.io.Serializable

fun timeOverlap(fromTimestamp: Long, toTimestamp: Long, from: Long, to: Long, timeaware: Boolean = true): Boolean {
    if (!timeaware) return true
    return if (fromTimestamp != toTimestamp) {
        !(to < fromTimestamp || from >= toTimestamp)
    } else {
        !(to < fromTimestamp || from > toTimestamp)
    }
}

interface Elem : Serializable {
    val id: Number
    val fromTimestamp: Long
    var toTimestamp: Long
    fun timeOverlap(timeaware: Boolean, from: Long, to: Long): Boolean = timeOverlap(fromTimestamp, toTimestamp, from, to, timeaware)
}
