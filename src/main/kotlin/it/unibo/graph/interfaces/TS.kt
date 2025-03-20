package it.unibo.graph.interfaces

import it.unibo.graph.utils.encodeBitwise
import java.io.Serializable

interface TS : Serializable {
    val g: Graph

    fun getTSId(): Long
    fun add(label: Label, timestamp: Long, value: Long) = add(
        N(
            encodeBitwise(getTSId(), timestamp),
            label,
            timestamp = timestamp,
            value = value,
            fromTimestamp = timestamp,
            toTimestamp = timestamp,
            g = g
        )
    )

    fun add(n: N): N
    fun getValues(): List<N>
    fun get(id: Long): N
}