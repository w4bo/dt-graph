package it.unibo.graph.interfaces

<<<<<<< HEAD
import it.unibo.graph.interfaces.Labels.HasTS
=======
>>>>>>> feat-tssingletable
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
<<<<<<< HEAD
    var nextRel: Long? = null, // link to the next edge, else null
=======
    var nextEdge: Long? = null, // link to the next edge, else null
>>>>>>> feat-tssingletable
    final override var nextProp: Long? = null, // link to the next property, else null
    val isTs : Boolean = false, // whether the node refers to an external TS

    // if TS event...
<<<<<<< HEAD
    @Transient val value: Long? = null, // value of the measurement, else null
    @Transient val relationships: MutableList<R> = mutableListOf(), // lists of edges towards the graph
=======
    @Transient val value: Long? = null, // value of the event, else null
    @Transient val edges: MutableList<R> = mutableListOf(), // lists of edges towards the graph
>>>>>>> feat-tssingletable
    @Transient final override val properties: MutableList<P> = mutableListOf(), // lists of node properties
    @Transient final override var g: Graph
) : ElemP {

<<<<<<< HEAD
=======
    // Secondary constructor: accepts label as String
    constructor(id: Long, labelName: String, fromTimestamp: Long = Long.MIN_VALUE, toTimestamp: Long = Long.MAX_VALUE, nextEdge: Long? = null, nextProp: Long? = null, isTs: Boolean = false, value: Long? = null, edges: MutableList<R> = mutableListOf(), properties: MutableList<P> = mutableListOf(), g: Graph)
            : this(id, labelFromString(labelName), fromTimestamp, toTimestamp, nextEdge, nextProp, isTs, value, edges, properties, g)

>>>>>>> feat-tssingletable
    companion object {
        fun fromByteArray(bytes: ByteArray, g: Graph): N {
            val buffer = ByteBuffer.wrap(bytes)
            val id = buffer.long
            val fromTimestamp = buffer.long
            val toTimestamp = buffer.long
            val labelOrdinal = buffer.int
<<<<<<< HEAD
            val label = Labels.entries[labelOrdinal] // Convert ordinal to enum
            val nextProp = buffer.long.let { if (it == Long.MIN_VALUE) null else it }
            val nextRel = buffer.long.let { if (it == Long.MIN_VALUE) null else it }
            val isTs = buffer.get() != 0.toByte()
            return N(id = id, label = label, nextRel = nextRel, nextProp = nextProp, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, isTs = isTs, g = g)
        }

        fun createVirtualN(label: Label, aggregatedValue: Any, fromTimestamp: Long, toTimestamp: Long, g: Graph, properties: List<P> = emptyList()) = createVirtualN(label, mutableListOf(P(DUMMY_ID, DUMMY_ID, NODE, key = VALUE, value = aggregatedValue, type = PropType.DOUBLE, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)) + properties, fromTimestamp, toTimestamp, g)

        fun createVirtualN(label: Label, properties: List<P>, fromTimestamp: Long, toTimestamp: Long, g: Graph) = N(id = DUMMY_ID, label = label, nextRel = null, nextProp = null, relationships = mutableListOf(), properties = properties.toMutableList(), fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)
=======
            val label = Label.entries[labelOrdinal]!! // Convert ordinal to enum
            val nextProp = buffer.long.let { if (it == Long.MIN_VALUE) null else it }
            val nextEdge = buffer.long.let { if (it == Long.MIN_VALUE) null else it }
            val isTs = buffer.get() != 0.toByte()
            return N(id = id, label = label, nextEdge = nextEdge, nextProp = nextProp, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, isTs = isTs, g = g)
        }

        @Deprecated("Should be treated as a custom node builder")
        fun createVirtualN(label: Label, aggregatedValue: Any, fromTimestamp: Long, toTimestamp: Long, g: Graph, properties: List<P> = emptyList()) = createVirtualN(label, mutableListOf(P(DUMMY_ID, DUMMY_ID, NODE, key = VALUE, value = aggregatedValue, type = PropType.DOUBLE, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)) + properties, fromTimestamp, toTimestamp, g)

        @Deprecated("Should be treated as a custom node builder")
        fun createVirtualN(label: Label, properties: List<P>, fromTimestamp: Long, toTimestamp: Long, g: Graph) = N(id = DUMMY_ID, label = label, nextEdge = null, nextProp = null, edges = mutableListOf(), properties = properties.toMutableList(), fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)
>>>>>>> feat-tssingletable
    }

    fun serialize(): ByteArray {
        val buffer = ByteBuffer.allocate(NODE_SIZE)
        buffer.putLong(id)                                // 8 bytes
        buffer.putLong(fromTimestamp)                     // 8 bytes
        buffer.putLong(toTimestamp)                       // 8 bytes
<<<<<<< HEAD
        buffer.putInt((label as Labels).ordinal)          // 4 bytes
        buffer.putLong(nextProp?: Long.MIN_VALUE)         // 8 bytes
        buffer.putLong(nextRel?: Long.MIN_VALUE)          // 8 bytes
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
=======
        buffer.putInt(label.ordinal)                      // 4 bytes
        buffer.putLong(nextProp?: Long.MIN_VALUE)         // 8 bytes
        buffer.putLong(nextEdge?: Long.MIN_VALUE)         // 8 bytes
        buffer.put(if (isTs) 1 else 0)                       // 5 byte
        return buffer.array()                                     // Total: 45 bytes
    }

    override fun getProps(next: Long?, filter: PropType?, name: String?, fromTimestamp: Long, toTimestamp: Long, timeaware: Boolean): List<P> {
        val result = when (name) {
            ID -> this.id to PropType.LONG
            LABEL -> this.label to PropType.STRING
            FROM_TIMESTAMP -> this.fromTimestamp to PropType.LONG
            TO_TIMESTAMP -> this.toTimestamp to PropType.LONG
            VALUE -> value?.let { it to PropType.DOUBLE }
            else -> null
        }
        return result
            ?.let { (v, t) -> listOf(P(DUMMY_ID, id, NODE, name!!, v, t, g = g, fromTimestamp = this.fromTimestamp, toTimestamp = this.toTimestamp))}
            ?: super.getProps(next, filter, name, fromTimestamp, toTimestamp, timeaware)
>>>>>>> feat-tssingletable
    }

    fun getTS(): List<N> {
        return g.getTSM().getTS(id + 1).getValues(emptyList(), emptyList())
    }

<<<<<<< HEAD
    fun getRels(
        next: Long? = nextRel,
=======
    fun getEdges(
        next: Long? = nextEdge,
>>>>>>> feat-tssingletable
        direction: Direction? = null,
        label: Label? = null,
        includeHasTs: Boolean = false
    ): List<R> {
        val result = mutableListOf<R>()
        // Caso evento TS (relazioni già presenti)
<<<<<<< HEAD
        if (relationships != null && relationships.isNotEmpty()) {
            when (direction) {
                Direction.IN -> throw NotImplementedException()
                else -> return relationships.filter { label == null || it.label == label }
=======
        if (edges != null && edges.isNotEmpty()) {
            when (direction) {
                Direction.IN -> throw NotImplementedException()
                else -> return edges.filter { label == null || it.label == label }
>>>>>>> feat-tssingletable
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
<<<<<<< HEAD
        return "(id: $id, type: $label, from: $fromTimestamp, to: $toTimestamp)"
=======
        return "(id: $id, label: $label, from: $fromTimestamp, to: $toTimestamp)"
>>>>>>> feat-tssingletable
    }

    override fun equals(other: Any?): Boolean {
        if (other !is N) return false
<<<<<<< HEAD
        return id == other.id && label == other.label && nextRel == other.nextRel && nextProp == other.nextProp && value == other.value && isTs == other.isTs
    }

    override fun hashCode(): Int {
        return Objects.hash(id, label, nextRel, nextProp, value, isTs)
=======
        return id == other.id && label == other.label && nextEdge == other.nextEdge && nextProp == other.nextProp && value == other.value && isTs == other.isTs
    }

    override fun hashCode(): Int {
        return Objects.hash(id, label, nextEdge, nextProp, value, isTs)
>>>>>>> feat-tssingletable
    }
}