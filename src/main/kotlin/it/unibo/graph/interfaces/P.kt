package it.unibo.graph.interfaces

import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.GRAPH_SOURCE
import it.unibo.graph.utils.NODE
import it.unibo.graph.utils.decodeBitwiseSource
import org.locationtech.jts.io.WKTReader
import org.rocksdb.*
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.*
import kotlin.math.min

enum class PropType { INT, LONG, DOUBLE, STRING, GEOMETRY, NULL }

const val MAX_LENGTH_KEY = 24
const val MAX_LENGTH_VALUE = 8
const val PROPERTY_SIZE: Int = 41 + MAX_LENGTH_KEY + MAX_LENGTH_VALUE

open class P(
    final override val id: Int,
    val sourceId: Long,
    val sourceType: Boolean,
    val key: String,
    val value: Any,
    val type: PropType,
    var next: Int? = null,
    final override var fromTimestamp: Long = Long.MIN_VALUE,
    final override var toTimestamp: Long = Long.MAX_VALUE,
    @Transient final override var g: Graph,
    @Transient val fromDisk: Boolean = false
) : Elem {

    companion object {
        private val db: RocksDB
        private val DB_NAME = "db_properties"
        init {
            val options: DBOptions = DBOptions()
            options.setCreateIfMissing(true)
            options.setCreateMissingColumnFamilies(true)
            val cfDescriptors = listOf(ColumnFamilyDescriptor("default".toByteArray(), ColumnFamilyOptions()))
            val cfHandles: List<ColumnFamilyHandle> = ArrayList()
            db = RocksDB.open(options, DB_NAME, cfDescriptors, cfHandles)
        }

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
            val key = readString(buffer, MAX_LENGTH_KEY)
            val typeOrdinal = buffer.int
            val type = PropType.entries[typeOrdinal]
            val value: Any = when (type) {
                PropType.LONG -> buffer.long      // Serialize Long as 8 bytes
                PropType.DOUBLE -> buffer.double // Serialize Double as 8 bytes
                PropType.INT -> {                // Serialize Int as 4 bytes + 4 bytes as padding
                    buffer.int
                    buffer.int
                }
                PropType.STRING -> {             // Serialize String (using your serializeString method)
                    val v = readString(buffer, MAX_LENGTH_VALUE)
                    v.ifEmpty {
                        String(db.get("$id|$key".toByteArray()), Charsets.UTF_8)
                    }
                }
                PropType.GEOMETRY -> {
                    WKTReader().read(String(db.get("$id|$key".toByteArray()), Charsets.UTF_8))
                }
                else -> throw IllegalArgumentException("Unsupported type: $type")
            }
            val next = buffer.int.let { if (it == Int.MIN_VALUE) null else it }
            return P(id, sourceId, sourceType, key, value, type, next, fromTimestamp, toTimestamp, g = g, fromDisk = true)
        }
    }

    fun serialize(): ByteArray {
        fun serializeString(s: String, size: Int): ByteArray {
            val sBuffer = ByteBuffer.allocate(size)
            val bytes = s.toByteArray(StandardCharsets.UTF_8)
            val truncated = bytes.copyOfRange(0, minOf(bytes.size, size)) // Trim if too long
            sBuffer.put(truncated)
            return sBuffer.array()
        }
        val buffer = ByteBuffer.allocate(PROPERTY_SIZE)
        buffer.putLong(id.toLong())                     // 8 bytes
        buffer.putLong(fromTimestamp)                   // 8 bytes
        buffer.putLong(toTimestamp)                     // 8 bytes
        buffer.putLong(sourceId)                        // 8 bytes
        buffer.put(if (sourceType) 1 else 0)            // 1 Byte
        buffer.put(serializeString(key, MAX_LENGTH_KEY))// MAX_LENGTH_KEY Bytes
        buffer.putInt(type.ordinal)                     // 4 bytes
        when (type) {
            PropType.LONG -> buffer.putLong(value as Long)       // Serialize Long as 8 bytes
            PropType.DOUBLE -> buffer.putDouble(value as Double) // Serialize Double as 8 bytes
            PropType.INT -> {                                    // Serialize Int as 4 bytes + 4 bytes as padding
                buffer.putInt(0)
                buffer.putInt(value as Int)
            }
            PropType.STRING -> {                                 // Serialize String
                val value = value as String
                if (value.length > MAX_LENGTH_VALUE) { // in place if the string is longer than 8 bytes
                    db.put("$id|$key".toByteArray(), value.toByteArray(StandardCharsets.UTF_8))
                    repeat(8) { buffer.put(0.toByte()) } // Add 8 empty bytes (0x00)
                } else { // in place if the string is shorter than or equal to 8 bytes
                    buffer.put(serializeString(value, MAX_LENGTH_VALUE))
                }
            }
            PropType.GEOMETRY -> {
                val value = value.toString()
                db.put("$id|$key".toByteArray(), value.toByteArray(StandardCharsets.UTF_8))
            }
            else -> throw IllegalArgumentException("Unsupported type: $type")
        }
        buffer.putInt(next?: Int.MIN_VALUE)       // 4 bytes
        return buffer.array()                           // 41 + MAX_LENGTH_KEY + MAX_LENGTH_VALUE
    }

    init {
        if (!fromDisk && id != DUMMY_ID && decodeBitwiseSource(sourceId) == GRAPH_SOURCE) { // If the source is the graph and the property is not created at runtime
            val elem: ElemP = if (sourceType == NODE) g.getNode(sourceId) else g.getEdge(sourceId.toInt()) // get the source of the property (either an edge or a node)
            assert(elem.fromTimestamp <= fromTimestamp && toTimestamp <= elem.toTimestamp) { "${elem}\n${toString()}" }
            if (elem.nextProp == null) elem.nextProp = id // if the element already has no properties, do nothing
            else {
                 // TODO we only update the `toTimestamp` of properties of elements of the graph (and not of the TS)
                 // TODO here we iterate all the properties of the element, but maybe we could exploit some sorting over time
                 val oldP: P? = elem.getProps(name = key).filter { it.toTimestamp > fromTimestamp && it.fromTimestamp < fromTimestamp}.maxByOrNull { it.toTimestamp } // check if the element contains a property with the same name
                 if (oldP != null) { // if so, and if the `toTimestamp` is \ge than `fromTimestamp`
                     oldP.toTimestamp = fromTimestamp // update the previous `toTimestamp`
                     g.addProperty(oldP) // update the property
                 }
                 next = elem.nextProp  // update the next pointer of the node
                 elem.nextProp = id

                // var prev: P? = null
                // var cur: P = g.getProp(elem.nextProp!!)
                // var pointerUpdated = false
                // var lastP: P? = null
                // if (cur.key == key && cur.toTimestamp != Long.MAX_VALUE && cur.toTimestamp >= fromTimestamp && cur.fromTimestamp <= toTimestamp)
                //     throw IllegalArgumentException("You are trying to add overlapping properties!")
                // while (true) {
                //     if (cur.key == key) {
                //         lastP = cur
                //     }
                //     if (cur.toTimestamp <= toTimestamp) {
                //         // if (cur.toTimestamp == toTimestamp && cur.fromTimestamp > fromTimestamp)
                //         //     throw IllegalArgumentException("The new property must be more recent")
                //         if (prev == null) { // this is the first property to add
                //             if (!pointerUpdated) {
                //                 next = elem.nextProp  // update the next pointer of the node
                //                 elem.nextProp = id
                //                 pointerUpdated = true
                //
                //             }
                //         } else { // there is a previous property to modify
                //             if (!pointerUpdated) {
                //                 next = prev.next
                //                 prev.next = id
                //                 g.addProperty(prev) // update the property
                //                 pointerUpdated = true
                //             }
                //             if (cur.key == key) {
                //                 cur.toTimestamp = fromTimestamp
                //                 g.addProperty(cur) // update the property
                //                 break
                //             }
                //         }
                //     }
                //     if (cur.next == null) break
                //     prev = cur
                //     cur = g.getProp(cur.next!!)
                // }
                // if (!pointerUpdated) {
                //     cur.next = id
                //     g.addProperty(cur)
                //     if (lastP != null) {
                //         lastP.fromTimestamp = toTimestamp
                //         if (cur != lastP)
                //             g.addProperty(lastP)
                //     }
                // }
            }

            if (sourceType == NODE) g.addNode(elem as N) else g.addEdge(elem as R) // store the element again
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $sourceId, key: $key, value: $value, type: $type, from: $fromTimestamp, to: $toTimestamp}"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is P) return false
        return id == other.id && key == other.key && value == other.value && sourceId == other.sourceId && type == other.type && fromTimestamp == other.fromTimestamp && toTimestamp == other.toTimestamp
    }

    override fun hashCode(): Int {
        return Objects.hash(id, key, value, sourceId, type, fromTimestamp, toTimestamp)
    }
}