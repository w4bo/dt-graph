package it.unibo.graph.interfaces

import it.unibo.graph.utils.encodeBitwise
import java.io.Serializable

interface TS : Serializable {
    fun getTSId(): Long
    fun add(label: String, timestamp: Long, value: Long) = add(
        N(
            encodeBitwise(getTSId(), timestamp),
            label,
            timestamp = timestamp,
            value = value,
            fromTimestamp = timestamp,
            toTimestamp = timestamp
        )
    )

    fun add(n: N): N
    fun getValues(): List<N>
    fun get(id: Long): N
}