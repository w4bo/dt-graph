package it.unibo.graph

import it.unibo.graph.structure.CustomVertex
import org.rocksdb.RocksDB
import java.io.Serializable

interface TS: Serializable {
    fun getTSId(): Long
    fun add(label:String, timestamp: Long, value: Long) = add(N(encodeBitwise(getTSId(), timestamp), label, timestamp=timestamp, value=value))
    fun add(n: N): N
    fun getValues(): List<N>
    fun get(id: Long): N
}

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

class RocksDBTS(val id: Long, val db: RocksDB) : TS {
    override fun getTSId(): Long = id

    override fun add(n: N): N {
        db.put("$id|${n.timestamp}".toByteArray(), serialize(n))
        return n
    }

    override fun getValues(): List<N> {
        val acc: MutableList<N> = mutableListOf()
        val iterator = db.newIterator()
        iterator.seek("$id|".toByteArray())
        while (iterator.isValid) {
            val key = String(iterator.key())
            if (!key.startsWith("$id|")) break
            acc += deserialize<N>(iterator.value())
            iterator.next()
        }
        return acc
    }

    override fun get(timestamp: Long): N = deserialize(db.get("$id|${timestamp}".toByteArray()))
}

class CustomTS(ts: TS): TS by ts {
    override fun add(label: String, timestamp: Long, value: Long): N {
        return add(CustomVertex(timestamp, label, timestamp = timestamp, value = value))
    }
}