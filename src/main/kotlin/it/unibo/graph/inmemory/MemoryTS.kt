package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.*
import it.unibo.graph.query.*
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.VALUE
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
                    // TODO: we assume the properties in a measurement to be only instantaneous
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
            if (aggregationOperators.size > 1) throw IllegalArgumentException("More than one aggregation operator: $aggregationOperators")
            val groupby = by.filter { it.operator == null }
            if (groupby.size > 1) throw IllegalArgumentException("More than one attribute to group by: $groupby")
            return filteredValues
                .map { n -> Path(listOf(n), n.fromTimestamp, n.toTimestamp) }
                .flatMap { row ->
                    replaceTemporalProperties(row, emptyList(), by, mapOf(by.first().n to Pair(0, 0)), from = row.from, to = row.to, timeaware = true)
                }
                .groupBy { row ->
                    groupby.map {
                        val properties = row.result[0].getProps(name = it.property, fromTimestamp = row.from, toTimestamp = row.to)
                        if (properties.size > 1) throw IllegalArgumentException("More than one property: $properties")
                        properties.first().value
                    }
                }
                .map {
                    val values = it.value.map { path ->
                        if (path.result.size > 1) { throw IllegalArgumentException("Too many nodes: $path") }
                        val n = path.result[0]
                        (n.getProps(name = VALUE, fromTimestamp = path.from, toTimestamp = path.to).first().value as Number).toDouble()
                    }
                    val value = aggregateNumbers(values, aggregationOperators.first().operator!!, lastAggregation = false)
                    val g = filteredValues.first().g
                    val fromTimestamp = filteredValues.minOfOrNull { it.fromTimestamp }!!
                    val toTimestamp = filteredValues.maxOfOrNull { it.toTimestamp }!!
                    N.createVirtualN(filteredValues.first().label, value, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g,
                        groupby.zip(it.key).map {
                            P(DUMMY_ID, sourceId = DUMMY_ID.toLong(), key = it.first.property!!, value = it.second, type = PropType.STRING, sourceType = NODE, g = g, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp)
                        })
                }
        }
    }
    override fun get(id: Long): N = values[decodeBitwise(id).second]!!
}