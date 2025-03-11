package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.TS
import it.unibo.graph.utils.decodeBitwise

class MemoryTS(val id: Long) : TS {
    private val values: MutableMap<Long, N> = mutableMapOf()

    override fun getTSId(): Long = id

    override fun add(n: N): N {
        values[n.timestamp!!] = n
        return n
    }

    override fun getValues(): List<N> = values.values.toList()

    override fun get(id: Long): N = values[decodeBitwise(id).second]!!
}