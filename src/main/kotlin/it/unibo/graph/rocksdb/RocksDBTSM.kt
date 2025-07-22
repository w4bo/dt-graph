package it.unibo.graph.rocksdb

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
import org.rocksdb.Options
import org.rocksdb.RocksDB

class RocksDBTSM(override val g: Graph): TSManager {
    val db: RocksDB
    val DB_NAME = "db_ts"
    var id = 1

    init {
        val options = Options()
        options.setCreateIfMissing(true)
        options.setCreateMissingColumnFamilies(true)
        db = RocksDB.open(options, DB_NAME)
    }

    override fun getTS(id: Long): TS {
        return RocksDBTS(g, id, db)
    }

    override fun addTS(): TS {
        return RocksDBTS(g, nextTSId(), db)
    }

    override fun nextTSId(): Long = id++.toLong()

    override fun clear() {
        val iterator = db.newIterator()
        iterator.seekToFirst()
        while (iterator.isValid) {
            db.delete(iterator.key())
            iterator.next()
        }
        id = 1
    }
}
