package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.AggN
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.TS
import it.unibo.graph.query.*
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.decodeBitwise

class MemoryTS(override val g: Graph, val id: Long) : TS {
    private val values: MutableMap<Long, N> = mutableMapOf()

    override fun getTSId(): Long = id

    override fun add(n: N): N {
        values[n.fromTimestamp] = n
        return n
    }

    override fun getValues(by: List<Aggregate>, filters: List<Filter>): List<N> {
        val filteredValues = values.values.filter { node -> // for each node
            filters.all { filter -> // the node must fulfill all filters
                node.getProps(name = filter.property).any { // get the properties of the node
                    assert(it.fromTimestamp == it.toTimestamp) { it.toString() }
                    // TODO: we consider the properties in a measurement to be instantaneous
                    if (filter.attrFirst) {
                        Compare.apply(it.value, filter.value, filter.operator) // check that the node is ok
                    } else {
                        Compare.apply(filter.value, it.value, filter.operator)
                    }
                }
            }
        }
        if (by.isEmpty()) {
            return filteredValues
        } else {
            val aggregationOperators = by.filter { it.operator != null }
            if (aggregationOperators.size > 1) throw IllegalArgumentException("More than one aggregation operator")
            val value = aggregateNumbers(filteredValues.map { it.value?.toDouble() ?: throw IllegalArgumentException("NaN") }, aggregationOperators.first().operator!!, lastAggregation = false)
            val fromTimestamp = filteredValues.minOfOrNull { it.fromTimestamp }!!
            val toTimestamp = filteredValues.maxOfOrNull { it.toTimestamp }!!
            return listOf(AggN(filteredValues.first().label, value, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, filteredValues.first().g))
        }
    }
    override fun get(id: Long): N = values[decodeBitwise(id).second]!!
}