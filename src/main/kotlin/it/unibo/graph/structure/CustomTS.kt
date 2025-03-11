package it.unibo.graph.structure

import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.TS
import it.unibo.graph.utils.encodeBitwise

class CustomTS(ts: TS) : TS by ts {
    override fun add(label: String, timestamp: Long, value: Long): N {
        return add(
            CustomVertex(
                encodeBitwise(getTSId(), timestamp),
                label,
                timestamp = timestamp,
                value = value,
                fromTimestamp = timestamp,
                toTimestamp = timestamp
            )
        )
    }
}