package it.unibo.graph

import org.rocksdb.*

interface TSManager {
    fun addTS(): TS
    fun nextTSId(): Int
    fun getTS(id: Int): TS
    fun clear()
}

class MemoryTSManager : TSManager {
    private val tss: MutableList<TS> = ArrayList()

    override fun getTS(id: Int): TS {
        return tss[id]
    }

    override fun addTS(): TS {
        val ts = CustomTS(MemoryTS(nextTSId()))
        tss += ts
        return ts
    }

    override fun nextTSId(): Int = tss.size

    override fun clear() {
        tss.clear()
    }
}

class RocksDBTSM : TSManager {
    val db: RocksDB
    val DB_NAME = "db_ts"
    var id = 0

    init {
        val options = Options()
        options.setCreateIfMissing(true)
        options.setCreateMissingColumnFamilies(true)
        db = RocksDB.open(options, DB_NAME)
    }

    override fun getTS(id: Int): TS {
        return CustomTS(RocksDBTS(id, db))
    }

    override fun addTS(): TS {
        return CustomTS(RocksDBTS(nextTSId(), db))
    }

    override fun nextTSId(): Int = id++

    override fun clear() {
        val iterator = db.newIterator()
        iterator.seekToFirst()
        while (iterator.isValid) {
            db.delete(iterator.key())
            iterator.next()
        }
        id = 0
    }
}