package it.unibo.graph.interfaces

import java.io.Serializable

fun timeOverlap(fromTimestamp: Long, toTimestamp: Long, from: Long, to: Long, timeaware: Boolean = true): Boolean {
    if (!timeaware) return true
    val f = fromTimestamp.coerceAtLeast(from)
    val t = toTimestamp.coerceAtMost(to)
    return f < t || (f == t && (fromTimestamp == toTimestamp || from == to))
}

interface Elem : Serializable {
    val id: Number
    val fromTimestamp: Long
    var toTimestamp: Long
    fun timeOverlap(timeaware: Boolean, from: Long, to: Long): Boolean = timeOverlap(fromTimestamp, toTimestamp, from, to, timeaware)
}
