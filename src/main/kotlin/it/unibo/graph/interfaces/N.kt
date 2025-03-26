package it.unibo.graph.interfaces

import it.unibo.graph.interfaces.Labels.HasTS
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.VALUE
import org.apache.commons.lang3.NotImplementedException
import java.nio.ByteBuffer
import java.util.*

val NODE_SIZE: Int = 52

open class N(
    final override val id: Long, // node id
    final override val label: Label, // node label
    var nextRel: Int? = null, // if graph node, link to the next relationship
    final override var nextProp: Int? = null, // if graph node, link to the next property
    val value: Long? = null, // if TS snapshot: value of the measurement, else if TS node: pointer to the id of the TS
    val timestamp: Long? = null, // if TS snapshot: timestamp of the measurement
    @Transient val relationships: MutableList<R> = mutableListOf(), // if TS snapshot, lists of relationships towards the graph
    @Transient final override val properties: MutableList<P> = mutableListOf(), // if TS snapshot, lists of properties
    final override val fromTimestamp: Long = Long.MIN_VALUE,
    final override var toTimestamp: Long = Long.MAX_VALUE,
    @Transient final override var g: Graph
) : ElemP {

    @Transient var sum: Double? = null

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
            val timestamp = buffer.long.let { if (it == Long.MIN_VALUE) null else it }
            return N(id, label, nextRel = nextRel, nextProp = nextProp, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, timestamp = timestamp, value = value, g = g)
        }
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
        buffer.putLong(timestamp?: Long.MIN_VALUE)  // 8 bytes
        return buffer.array()                             // Total: 52 bytes
    }

    override fun getProps(next: Int?, filter: PropType?, name: String?, fromTimestamp: Long, toTimestamp: Long, timeaware: Boolean): List<P> {
        return when (name) {
            VALUE -> if (value != null) listOf(P(DUMMY_ID, id, NODE, VALUE, value, PropType.DOUBLE, g = g)) else emptyList()
            else -> {
                super.getProps(next, filter, name, fromTimestamp, toTimestamp, timeaware)
            }
        }
    }

    fun getTS(): List<N> {
        return g.getTSM().getTS(value!!).getValues(emptyList(), emptyList())
    }

    fun getRels(next: Int? = nextRel, direction: Direction? = null, label: Label? = null, includeHasTs: Boolean = false): List<R> {
        return if (label == HasTS) { // Jump to the time series
            listOf(R(DUMMY_ID, HasTS, id, value!!, g = g))
        } else { // Iterate within the graph
            if (timestamp != null) { // If TS snapshot
                when (direction) {
                    Direction.IN -> throw NotImplementedException()
                    else -> relationships.filter { label == null || it.label == label }.toList()
                }
            } else { // Graph node
                if (next == null) return if(includeHasTs && value != null) listOf(R(DUMMY_ID, HasTS, id, value, g = g)) else emptyList()
                val r = g.getEdge(next)
                if (label == null || r.label == label) {
                    when (direction) {
                        Direction.IN -> if (r.toN == id) listOf(r) else emptyList()
                        Direction.OUT -> if (r.fromN == id) listOf(r) else emptyList()
                        else -> listOf(r)
                    }
                } else {
                    emptyList()
                } + getRels(if (r.fromN == id) r.fromNextRel else r.toNextRel, direction, label, includeHasTs)
            }
        }
    }

    override fun toString(): String {
        return "(id: $id, type: $label, from: $fromTimestamp, to: $toTimestamp)"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is N) return false
        return id == other.id && label == other.label
    }

    override fun hashCode(): Int {
        return Objects.hash(id, label)
    }
}