package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.TS
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Compare
import it.unibo.graph.query.Filter
import it.unibo.graph.utils.decodeBitwise

class MemoryTS(override val g: Graph, val id: Long) : TS {
    private val values: MutableMap<Long, N> = mutableMapOf()

    override fun getTSId(): Long = id

    override fun add(n: N): N {
        values[n.fromTimestamp] = n
        return n
    }

    override fun getValues(by: List<Aggregate>, filters: List<Filter>): List<N> =
        values.values.filter { node -> // for each node
            filters.all { filter -> // the node must fulfill all filters
                node.getProps(name = filter.property).any { // get the properties of the node
                    assert(it.fromTimestamp == it.toTimestamp){ it.toString() } // TODO: we consider the properties in a measurement to be instantaneous
                    Compare.apply(it.value, filter.value, filter.operator) // check that the node is ok
                }
            }
        }

    override fun get(id: Long): N = values[decodeBitwise(id).second]!!
}