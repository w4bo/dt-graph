package it.unibo.graph.interfaces

import java.io.Serializable

interface Elem: Serializable {
    val id: Number
    val fromTimestamp: Long
    var toTimestamp: Long
    fun timeOverlap(timeaware: Boolean, from: Long, to: Long): Boolean = !timeaware || !(to < fromTimestamp || from >= toTimestamp)
}
