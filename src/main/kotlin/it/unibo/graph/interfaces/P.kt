package it.unibo.graph.interfaces

import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.GRAPH_SOURCE
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.decodeBitwiseSource
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.*

enum class PropType { INT, DOUBLE, STRING, TS, GEOMETRY }

const val MAX_LENGTH_STRING = 24

val PROPERTY_SIZE: Int = 41 * MAX_LENGTH_STRING * 2

open class P(
    final override val id: Int,
    val sourceId: Long,
    val sourceType: Boolean,
    val key: String,
    val value: Any,
    val type: PropType,
    var next: Int? = null,
    final override val fromTimestamp: Long = Long.MIN_VALUE,
    final override var toTimestamp: Long = Long.MAX_VALUE,
    @Transient final override var g: Graph
) : Elem {

    companion object {
        fun fromByteArray(bytes: ByteArray, g: Graph): P {
            val buffer = ByteBuffer.wrap(bytes)
            val id = buffer.long.toInt()
            val fromTimestamp = buffer.long
            val toTimestamp = buffer.long
            val sourceId = buffer.long
            val sourceType = buffer.get().toInt() == 1
            // Read fixed-length strings
            fun readString(buffer: ByteBuffer, length: Int): String {
                val bytes = ByteArray(length)
                buffer.get(bytes)
                return bytes.toString(StandardCharsets.UTF_8).trimEnd('\u0000') // Trim null padding
            }
            val key = readString(buffer, MAX_LENGTH_STRING)
            val valueStr = readString(buffer, MAX_LENGTH_STRING) // Read as string first
            val typeOrdinal = buffer.int
            val type = PropType.entries[typeOrdinal]
            val next = buffer.int.let { if (it == Int.MIN_VALUE) null else it }
            // // Convert value based on PropType
            // val value: Any = when (type) {
            //     PropType.STRING -> valueStr
            //     PropType.INTEGER -> valueStr.toIntOrNull() ?: 0
            //     PropType.FLOAT -> valueStr.toFloatOrNull() ?: 0f
            //     PropType.BOOLEAN -> valueStr.toBooleanStrictOrNull() ?: false
            // }
            val value = valueStr
            return P(id, sourceId, sourceType, key, value, type, next, fromTimestamp, toTimestamp, g = g)
        }
    }

    fun serialize(): ByteArray {
        fun serializeString(s: String): ByteArray {
            val sBuffer = ByteBuffer.allocate(MAX_LENGTH_STRING)
            val bytes = s.toByteArray(StandardCharsets.UTF_8)
            val truncated = bytes.copyOfRange(0, minOf(bytes.size, MAX_LENGTH_STRING)) // Trim if too long
            sBuffer.put(truncated)
            return sBuffer.array()
        }
        val buffer = ByteBuffer.allocate(PROPERTY_SIZE)
        buffer.putLong(id.toLong())                     // 8 bytes
        buffer.putLong(fromTimestamp)                   // 8 bytes
        buffer.putLong(toTimestamp)                     // 8 bytes
        buffer.putLong(sourceId)                        // 8 bytes
        buffer.put(if (sourceType) 1 else 0)            // 1 Byte
        buffer.put(serializeString(key))                // MAX_LENGTH_STRING Bytes
        buffer.put(serializeString(value.toString()))   // MAX_LENGTH_STRING Bytes
        buffer.putInt(type.ordinal)                     // 4 bytes
        buffer.putInt(next?: Int.MIN_VALUE)       // 4 bytes
        return buffer.array()                           // 41 + MAX_LENGTH_STRING * 2
    }

    init {
        if (decodeBitwiseSource(sourceId) == GRAPH_SOURCE && id != DUMMY_ID) { // If the source is the graph and the property is not created at runtime
            val elem: ElemP = if (sourceType == NODE) g.getNode(sourceId) else g.getEdge(sourceId.toInt()) // get the source of the property (either an edge or a node)
            if (elem.nextProp == null) elem.nextProp = id // if the element already has no properties, do nothing
            else {
                // TODO we only update the `toTimestamp` of properties of elements of the graph (and not of the TS)
                // TODO here we iterate all the properties of the element, but maybe we could exploit some sorting over time
                val oldP: P? = elem.getProps(name = key).filter { it.toTimestamp > fromTimestamp }.maxByOrNull { it.toTimestamp } // check if the element contains a property with the same name
                if (oldP != null) { // if so, and if the `toTimestamp` is ge than `fromTimestamp`
                    oldP.toTimestamp = fromTimestamp // update the previous `toTimestamp`
                    g.addProperty(oldP) // update the property
                }
                next = elem.nextProp  // update the next pointer of the node
                elem.nextProp = id
            }
            // if (elem is N && key == LOCATION) {
            //     elem.location = GeoJsonReader().read(value.toString())
            //     elem.locationTimestamp = fromTimestamp
            // }
            if (sourceType == NODE) g.addNode(elem as N) else g.addEdge(elem as R) // store the element again
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $sourceId, key: $key, value: $value, type: $type, from: $fromTimestamp, to: $toTimestamp}"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is P) return false
        return id == other.id && key == other.key && value == other.value && sourceId == other.sourceId && type == other.type
    }

    override fun hashCode(): Int {
        return Objects.hash(id, key, value, sourceId, type)
    }
}