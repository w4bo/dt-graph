package it.unibo.graph.interfaces

import it.unibo.graph.interfaces.Labels.HasTS
import it.unibo.graph.utils.*
import org.apache.commons.lang3.NotImplementedException
import java.nio.ByteBuffer
import java.util.*

const val NODE_SIZE: Int = 45 // byte

open class N(
    final override val id: Long, // node id
    final override val label: Label, // node label
    final override val fromTimestamp: Long = Long.MIN_VALUE, // minimum validity
    final override var toTimestamp: Long = Long.MAX_VALUE, // maximum validity

    // if graph node...
    var nextEdge: Long? = null, // link to the next edge, else null
    final override var nextProp: Long? = null, // link to the next property, else null
    val isTs : Boolean = false, // whether the node refers to an external TS

    // if TS event...
    @Transient val value: Long? = null, // value of the event, else null
    @Transient val edges: MutableList<R> = mutableListOf(), // lists of edges towards the graph
    @Transient final override val properties: MutableList<P> = mutableListOf(), // lists of node properties
    @Transient final override var g: Graph
) : ElemP {

    companion object {
        fun fromByteArray(bytes: ByteArray, g: Graph): N {
            val buffer = ByteBuffer.wrap(bytes)
            val id = buffer.long
            val fromTimestamp = buffer.long
            val toTimestamp = buffer.long
            val labelOrdinal = buffer.int
            val label = Labels.entries[labelOrdinal] // Convert ordinal to enum
            val nextProp = buffer.long.let { if (it == Long.MIN_VALUE) null else it }
            val nextEdge = buffer.long.let { if (it == Long.MIN_VALUE) null else it }
            val isTs = buffer.get() != 0.toByte()
            return N(id = id, label = label, nextEdge = nextEdge, nextProp = nextProp, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, isTs = isTs, g = g)
        }

        fun createVirtualN(label: Label, aggregatedValue: Any, fromTimestamp: Long, toTimestamp: Long, g: Graph, properties: List<P> = emptyList()) = createVirtualN(label, mutableListOf(P(DUMMY_ID, DUMMY_ID, NODE, key = VALUE, value = aggregatedValue, type = PropType.DOUBLE, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)) + properties, fromTimestamp, toTimestamp, g)

        fun createVirtualN(label: Label, properties: List<P>, fromTimestamp: Long, toTimestamp: Long, g: Graph) = N(id = DUMMY_ID, label = label, nextEdge = null, nextProp = null, edges = mutableListOf(), properties = properties.toMutableList(), fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)
    }

    fun serialize(): ByteArray {
        val buffer = ByteBuffer.allocate(NODE_SIZE)
        buffer.putLong(id)                                // 8 bytes
        buffer.putLong(fromTimestamp)                     // 8 bytes
        buffer.putLong(toTimestamp)                       // 8 bytes
        buffer.putInt((label as Labels).ordinal)          // 4 bytes
        buffer.putLong(nextProp?: Long.MIN_VALUE)         // 8 bytes
        buffer.putLong(nextEdge?: Long.MIN_VALUE)         // 8 bytes
        buffer.put(if (isTs) 1 else 0)                       // 5 byte
        return buffer.array()                                    // Total: 45 bytes
    }

    override fun getProps(next: Long?, filter: PropType?, name: String?, fromTimestamp: Long, toTimestamp: Long, timeaware: Boolean): List<P> {
        return when (name) {
            FROM_TIMESTAMP -> listOf(P(DUMMY_ID, id, NODE, FROM_TIMESTAMP, this.fromTimestamp, PropType.LONG, g = g, fromTimestamp = this.fromTimestamp, toTimestamp = this.toTimestamp))
            TO_TIMESTAMP -> listOf(P(DUMMY_ID, id, NODE, TO_TIMESTAMP, this.toTimestamp, PropType.LONG, g = g, fromTimestamp = this.fromTimestamp, toTimestamp = this.toTimestamp))
            VALUE -> value?.let { listOf(P(DUMMY_ID, id, NODE, VALUE, it, PropType.DOUBLE, g = g, fromTimestamp = this.fromTimestamp, toTimestamp = this.toTimestamp)) } ?: super.getProps(next, filter, name, fromTimestamp, toTimestamp, timeaware)
            else -> super.getProps(next, filter, name, fromTimestamp, toTimestamp, timeaware)
        }
    }

    fun getTS(): List<N> {
        return g.getTSM().getTS(id + 1).getValues(emptyList(), emptyList())
    }

    fun getRels(
        next: Long? = nextEdge,
        direction: Direction? = null,
        label: Label? = null,
        includeHasTs: Boolean = false
    ): List<R> {
        val result = mutableListOf<R>()
        // Caso evento TS (relazioni già presenti)
        if (edges != null && edges.isNotEmpty()) {
            when (direction) {
                Direction.IN -> throw NotImplementedException()
                else -> return edges.filter { label == null || it.label == label }
            }
        }
        // Caso nodo grafo: itera sulle relazioni collegate
        var current: Long? = next
        while (current != null) {
            val r = g.getEdge(current)
            if (label == null || r.label == label) {
                when (direction) {
                    Direction.IN -> if (r.toN == id) result.add(r)
                    Direction.OUT -> if (r.fromN == id) result.add(r)
                    else -> result.add(r)
                }
            }
            current = if (r.fromN == id) r.fromNextRel else r.toNextRel
        }
        // Se richiesto, aggiungi relazione HasTS
        if (includeHasTs && isTs) {
            result.add(R(DUMMY_ID, HasTS, id, id + 1, g = g))
        }
        return result
    }

    override fun toString(): String {
        return "(id: $id, type: $label, from: $fromTimestamp, to: $toTimestamp)"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is N) return false
        return id == other.id && label == other.label && nextEdge == other.nextEdge && nextProp == other.nextProp && value == other.value && isTs == other.isTs
    }

    override fun hashCode(): Int {
        return Objects.hash(id, label, nextEdge, nextProp, value, isTs)
    }
}