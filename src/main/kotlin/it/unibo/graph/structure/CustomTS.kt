package it.unibo.graph.structure

import it.unibo.graph.interfaces.Label
import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.TS
import it.unibo.graph.utils.encodeBitwise

class CustomTS(ts: TS) : TS by ts {
    override fun add(label: Label, timestamp: Long, value: Long): N {
        return add(
            CustomVertex(
                encodeBitwise(getTSId(), timestamp),
                label,
                value = value,
                fromTimestamp = timestamp,
                toTimestamp = timestamp,
                g = g
            )
        )
    }
}