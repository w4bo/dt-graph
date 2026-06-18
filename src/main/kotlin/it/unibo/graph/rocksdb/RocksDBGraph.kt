package it.unibo.graph.rocksdb

import it.unibo.graph.interfaces.*
<<<<<<< HEAD
import org.apache.commons.lang3.NotImplementedException
import org.rocksdb.*

class RocksDBGraph : Graph {

    companion object {
        private var db: RocksDB? = null
        private const val DB_NAME = "db_graph"
        var nodes: ColumnFamilyHandle? = null
        var edges: ColumnFamilyHandle? = null
        var properties: ColumnFamilyHandle? = null

        // Synchronized function to ensure thread safety
        @Synchronized
        fun getInstance(): RocksDB {
            if (db == null) {
                val options = DBOptions()
                options.setCreateIfMissing(true)
                options.setCreateMissingColumnFamilies(true)
                val cfDescriptors = listOf(
                    ColumnFamilyDescriptor("default".toByteArray(), ColumnFamilyOptions()),
                    ColumnFamilyDescriptor("nodes".toByteArray(), ColumnFamilyOptions()),
                    ColumnFamilyDescriptor("edges".toByteArray(), ColumnFamilyOptions()),
                    ColumnFamilyDescriptor("properties".toByteArray(), ColumnFamilyOptions())
                )
                val cfHandles: List<ColumnFamilyHandle> = ArrayList()
                db = RocksDB.open(options, DB_NAME, cfDescriptors, cfHandles)
                nodes = cfHandles[1]
                edges = cfHandles[2]
                properties = cfHandles[3]
            }
            return db!!
        }
    }
    override var tsm: TSManager? = null
    private val db = getInstance()
=======
import it.unibo.graph.utils.PATH
import org.apache.commons.lang3.NotImplementedException
import org.rocksdb.*
import java.io.File

class RocksDBGraph(override val path: String = PATH, override var dynamicDb: RocksDB? = null) : Graph {

    var nodes: ColumnFamilyHandle? = null
    var edges: ColumnFamilyHandle? = null
    var properties: ColumnFamilyHandle? = null
    val options = DBOptions()
    val cfHandles: List<ColumnFamilyHandle> = ArrayList()

    init {
        options.setCreateIfMissing(true)
        options.setCreateMissingColumnFamilies(true)
        val cfDescriptors = listOf(
            ColumnFamilyDescriptor("default".toByteArray(), ColumnFamilyOptions()),
            ColumnFamilyDescriptor("nodes".toByteArray(), ColumnFamilyOptions()),
            ColumnFamilyDescriptor("edges".toByteArray(), ColumnFamilyOptions()),
            ColumnFamilyDescriptor("properties".toByteArray(), ColumnFamilyOptions())
        )

        dynamicDb = RocksDB.open(options, "${path}/graph_rocksdb", cfDescriptors, cfHandles)
        nodes = cfHandles[1]
        edges = cfHandles[2]
        properties = cfHandles[3]
    }

    override var tsm: TSManager? = null
>>>>>>> feat-tssingletable
    private var nodeId = 0L
    private var edgeId = 0L
    private var propId = 0L

    override fun clear() {
        listOf(nodes, edges, properties).forEach {
<<<<<<< HEAD
            val iterator = db.newIterator(it)
            iterator.seekToFirst()
            while (iterator.isValid) {
                db.delete(it, iterator.key())
=======
            val iterator = dynamicDb!!.newIterator(it)
            iterator.seekToFirst()
            while (iterator.isValid) {
                dynamicDb!!.delete(it, iterator.key())
>>>>>>> feat-tssingletable
                iterator.next()
            }
        }
        nodeId = 0
        edgeId = 0
        propId = 0
    }

    override fun close() {
<<<<<<< HEAD
        // db.close()
        // options.close()
=======
        cfHandles.forEach { it.close() }
        dynamicDb?.closeE()
        options.close()
        File("$path/properties/LOCK").delete()
        tsm?.close()
>>>>>>> feat-tssingletable
    }

    override fun nextNodeId(): Long = nodeId++

    override fun nextPropertyId(): Long = propId++

    override fun nextEdgeId(): Long = edgeId++

    override fun addNode(n: N): N {
<<<<<<< HEAD
        db.put(nodes, "${n.id}".toByteArray(), n.serialize())
=======
        dynamicDb!!.put(nodes, "${n.id}".toByteArray(), n.serialize())
>>>>>>> feat-tssingletable
        return n
    }

    override fun addPropertyLocal(key: Long, p: P): P {
<<<<<<< HEAD
        db.put(properties, "${p.id}".toByteArray(), p.serialize())
=======
        dynamicDb!!.put(properties, "${p.id}".toByteArray(), p.serialize())
>>>>>>> feat-tssingletable
        return p
    }

    override fun addEdgeLocal(key: Long, r: R): R {
<<<<<<< HEAD
        db.put(edges, "${r.id}".toByteArray(), r.serialize())
=======
        dynamicDb!!.put(edges, "${r.id}".toByteArray(), r.serialize())
>>>>>>> feat-tssingletable
        return r
    }

    override fun getProps(): MutableList<P> {
        throw NotImplementedException()
    }

    override fun getNodes(): MutableList<N> {
        val acc: MutableList<N> = mutableListOf()
<<<<<<< HEAD
        val iterator = db.newIterator(nodes)
=======
        val iterator = dynamicDb!!.newIterator(nodes)
>>>>>>> feat-tssingletable
        iterator.seekToFirst()
        while (iterator.isValid) {
            acc += N.fromByteArray(iterator.value(), this)
            iterator.next()
        }
        return acc
    }

    override fun getEdges(): MutableList<R> {
        throw NotImplementedException()
    }

    override fun getProp(id: Long): P {
<<<<<<< HEAD
        val b = db.get(properties, "$id".toByteArray())
=======
        val b = dynamicDb!!.get(properties, "$id".toByteArray())
>>>>>>> feat-tssingletable
        return P.fromByteArray(b, this)
    }

    override fun getNode(id: Long): N {
<<<<<<< HEAD
        val b = db.get(nodes, "$id".toByteArray())
=======
        val b = dynamicDb!!.get(nodes, "$id".toByteArray())
>>>>>>> feat-tssingletable
        return N.fromByteArray(b, this)
    }

    override fun getEdge(id: Long): R {
<<<<<<< HEAD
        val b = db.get(edges, "$id".toByteArray())
=======
        val b = dynamicDb!!.get(edges, "$id".toByteArray())
>>>>>>> feat-tssingletable
        return R.fromByteArray(b, this)
    }
}
