package it.unibo.graph.interfaces

import it.unibo.graph.interfaces.Labels.HasTS
import it.unibo.graph.utils.*
import org.apache.commons.lang3.NotImplementedException
import java.nio.ByteBuffer
import java.util.*

const val NODE_SIZE: Int = 48

open class N(
    final override val id: Long, // node id
    final override val label: Label, // node label
    var nextRel: Int? = null, // if graph node, link to the next relationship
    final override var nextProp: Int? = null, // if graph node, link to the next property
    val value: Long? = null, // if TS snapshot: value of the measurement, else if TS node: pointer to the id of the TS
    @Transient val relationships: MutableList<R> = mutableListOf(), // if TS snapshot, lists of relationships towards the graph
    @Transient final override val properties: MutableList<P> = mutableListOf(), // if TS snapshot, lists of properties
    final override val fromTimestamp: Long = Long.MIN_VALUE,
    final override var toTimestamp: Long = Long.MAX_VALUE,
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
            val nextProp = buffer.int.let { if (it == Int.MIN_VALUE) null else it }
            val nextRel = buffer.int.let { if (it == Int.MIN_VALUE) null else it }
            val value = buffer.long.let { if (it == Long.MIN_VALUE) null else it }
            return N(id, label, nextRel = nextRel, nextProp = nextProp, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, value = value, g = g)
        }

        fun createVirtualN(label: Label, aggregatedValue: Any, fromTimestamp: Long, toTimestamp: Long, g: Graph, properties: List<P> = emptyList()) = createVirtualN(label, mutableListOf(P(DUMMY_ID, DUMMY_ID.toLong(), NODE, key = VALUE, value = aggregatedValue, type = PropType.DOUBLE, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)) + properties, fromTimestamp, toTimestamp, g)

        fun createVirtualN(label: Label, properties: List<P>, fromTimestamp: Long, toTimestamp: Long, g: Graph) = N(DUMMY_ID.toLong(), label, null, null, null, mutableListOf(), properties.toMutableList(), fromTimestamp, toTimestamp, g)
    }

    fun serialize(): ByteArray {
        val buffer = ByteBuffer.allocate(NODE_SIZE)
        buffer.putLong(id)                                // 8 bytes
        buffer.putLong(fromTimestamp)                     // 8 bytes
        buffer.putLong(toTimestamp)                       // 8 bytes
        buffer.putInt((label as Labels).ordinal)          // 4 bytes
        buffer.putInt(nextProp?: Int.MIN_VALUE)     // 4 bytes
        buffer.putInt(nextRel?: Int.MIN_VALUE)      // 4 bytes
        buffer.putLong(value?: Long.MIN_VALUE)      // 8 bytes
        return buffer.array()                             // Total: 48 bytes
    }

    override fun getProps(next: Int?, filter: PropType?, name: String?, fromTimestamp: Long, toTimestamp: Long, timeaware: Boolean): List<P> {
        return when (name) {
            FROM_TIMESTAMP -> listOf(P(DUMMY_ID, id, NODE, FROM_TIMESTAMP, this.fromTimestamp, PropType.LONG, g = g, fromTimestamp = this.fromTimestamp, toTimestamp = this.toTimestamp))
            TO_TIMESTAMP -> listOf(P(DUMMY_ID, id, NODE, TO_TIMESTAMP, this.toTimestamp, PropType.LONG, g = g, fromTimestamp = this.fromTimestamp, toTimestamp = this.toTimestamp))
            VALUE -> value?.let { listOf(P(DUMMY_ID, id, NODE, VALUE, it, PropType.DOUBLE, g = g, fromTimestamp = this.fromTimestamp, toTimestamp = this.toTimestamp)) } ?: super.getProps(next, filter, name, fromTimestamp, toTimestamp, timeaware)
            else -> super.getProps(next, filter, name, fromTimestamp, toTimestamp, timeaware)
        }
    }

    fun getTS(): List<N> {
        return g.getTSM().getTS(value!!).getValues(emptyList(), emptyList())
    }

    fun getRels(
        next: Int? = nextRel,
        direction: Direction? = null,
        label: Label? = null,
        includeHasTs: Boolean = false
    ): List<R> {
        // Caso speciale: label == HasTS
        if (label == HasTS) {
            return listOf(R(DUMMY_ID, HasTS, id, value!!, g = g))
        }

        val result = mutableListOf<R>()

        // Caso snapshot (relazioni già presenti)
        if (relationships != null && relationships.isNotEmpty()) {
            when (direction) {
                Direction.IN -> throw NotImplementedException()
                else -> return relationships.filter { label == null || it.label == label }
            }
        }

        // Caso nodo grafico: itera sulle relazioni collegate
        var current = next
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
        if (includeHasTs && value != null) {
            result.add(R(DUMMY_ID, HasTS, id, value, g = g))
        }

        return result
    }

    override fun toString(): String {
        return "(id: $id, type: $label, from: $fromTimestamp, to: $toTimestamp)"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is N) return false
        return id == other.id && label == other.label && nextRel == other.nextRel && nextProp == other.nextProp && value == other.value
    }

    override fun hashCode(): Int {
        return Objects.hash(id, label, nextRel, nextProp, value)
    }
}