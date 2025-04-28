package it.unibo.graph.structure

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Label
import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.TS
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.utils.encodeBitwise

class CustomTS(val ts: TS, override val g: Graph) : TS {
    override fun getTSId() = ts.getTSId()

    override fun createNode(label: Label, timestamp: Long, value: Long): N {
        return CustomVertex(encodeBitwise(getTSId(), timestamp), label, value = value, fromTimestamp = timestamp, toTimestamp = timestamp, g = g)
    }

    override fun add(n: N, isUpdate: Boolean) = ts.add(n, isUpdate)

    override fun getValues(by: List<Aggregate>, filters: List<Filter>): List<N> = ts.getValues(by, filters)

    override fun get(id: Long): N = ts.get(id)
}