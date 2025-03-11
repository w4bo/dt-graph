package it.unibo.graph.rocksdb

import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.TS
import it.unibo.graph.utils.deserialize
import it.unibo.graph.utils.serialize
import org.rocksdb.RocksDB

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