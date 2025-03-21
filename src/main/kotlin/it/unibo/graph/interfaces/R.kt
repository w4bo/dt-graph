package it.unibo.graph.interfaces

import it.unibo.graph.utils.DUMMY_ID
import java.nio.ByteBuffer
import java.util.*

enum class Direction { IN, OUT }

val EDGE_SIZE: Int = 56

open class R(
    final override val id: Int, // id of the relationship
    final override val label: Label, // label
    val fromN: Long, // from node
    val toN: Long, // to node
    var fromNextRel: Int? = null, // pointer to the next relationship of the `from node`
    var toNextRel: Int? = null, // pointer to the next relationship of the `to node`
    final override val fromTimestamp: Long = Long.MIN_VALUE,
    final override var toTimestamp: Long = Long.MAX_VALUE,
    final override var nextProp: Int? = null,
    @Transient final override val properties: MutableList<P> = mutableListOf(),
    @Transient final override var g: Graph
): ElemP {

    companion object {
        fun fromByteArray(bytes: ByteArray, g: Graph): R {
            val buffer = ByteBuffer.wrap(bytes)
            val id = buffer.long.toInt() // Read ID (stored as Long, convert to Int)
            val fromTimestamp = buffer.long
            val toTimestamp = buffer.long
            val labelOrdinal = buffer.int
            val label = Labels.entries[labelOrdinal] // Convert ordinal to enum
            val nextProp = buffer.int.let { if (it == Int.MIN_VALUE) null else it }
            val fromN = buffer.long
            val toN = buffer.long
            val fromNextRel = buffer.int.let { if (it == Int.MIN_VALUE) null else it }
            val toNextRel = buffer.int.let { if (it == Int.MIN_VALUE) null else it }
            return R(id, label, fromN, toN, fromNextRel, toNextRel, fromTimestamp, toTimestamp, nextProp, g = g)
        }
    }

    fun serialize(): ByteArray {
        val buffer = ByteBuffer.allocate(EDGE_SIZE)
        buffer.putLong(id.toLong())                       // 8 bytes
        buffer.putLong(fromTimestamp)                     // 8 bytes
        buffer.putLong(toTimestamp)                       // 8 bytes
        buffer.putInt((label as Labels).ordinal)          // 4 bytes
        buffer.putInt(nextProp?: Int.MIN_VALUE)     // 4 bytes
        buffer.putLong(fromN)                             // 8 bytes
        buffer.putLong(toN)                               // 8 bytes
        buffer.putInt(fromNextRel?: Int.MIN_VALUE)  // 4 bytes
        buffer.putInt(toNextRel?: Int.MIN_VALUE)    // 4 bytes
        return buffer.array()                             // Total: 56 bytes
    }

    init {
        if (id != DUMMY_ID) {
            val from = g.getNode(fromN)
            val to = g.getNode(toN)

            if (from.nextRel == null) { // this is the first edge
                from.nextRel = id
            } else {
                for (it in from.getRels(direction = Direction.OUT, label = label)) {
                    if (it.toN == toN && toTimestamp == Long.MAX_VALUE) {
                        it.toTimestamp = fromTimestamp
                        break
                    }
                }
                fromNextRel = from.nextRel
                from.nextRel = id
            }

            if (to.nextRel == null) { // this is the first edge
                to.nextRel = id
            } else {
                toNextRel = to.nextRel
                to.nextRel = id
            }

            g.addNode(from)
            g.addNode(to)
        }
    }

    override fun toString(): String {
        return "($fromN)-[id: $id, type: $label, from: $fromTimestamp, to: $toTimestamp}->($toN)"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is R) return false
        return id == other.id && label == other.label && fromN == other.fromN && toN == other.toN
    }

    override fun hashCode(): Int {
        return Objects.hash(id, label, fromN, toN)
    }
}