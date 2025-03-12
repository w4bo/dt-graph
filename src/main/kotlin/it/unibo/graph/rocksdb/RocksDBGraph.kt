package it.unibo.graph.rocksdb

import it.unibo.graph.interfaces.*
import it.unibo.graph.utils.deserialize
import it.unibo.graph.utils.serialize
import org.apache.commons.lang3.NotImplementedException
import org.locationtech.jts.io.geojson.GeoJsonReader
import org.rocksdb.*

class RocksDBGraph() : Graph {
    override var tsm: TSManager? = null
    val nodes: ColumnFamilyHandle
    val edges: ColumnFamilyHandle
    val properties: ColumnFamilyHandle
    val db: RocksDB
    var nodeId = 0L
    var edgeId = 0
    var propId = 0
    val DB_NAME = "db_graph"

    init {
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

    override fun clear() {
        listOf(nodes, edges, properties).forEach {
            val iterator = db.newIterator(it)
            iterator.seekToFirst()
            while (iterator.isValid) {
                db.delete(it, iterator.key())
                iterator.next()
            }
        }
        nodeId = 0
        edgeId = 0
        propId = 0
    }

    override fun nextNodeId(): Long = nodeId++

    override fun nextPropertyId(): Int = propId++

    override fun upsertFirstCitizenProperty(prop: P): P? {
        val nodeId = prop.sourceId
        val n = getNode(nodeId)
        n.location = GeoJsonReader().read(prop.value.toString())
        n.locationTimestamp = prop.fromTimestamp
        addNode(n)
        return prop
    }

    override fun nextEdgeId(): Int = edgeId++

    override fun addNode(n: N): N {
        db.put(nodes, "${n.id}".toByteArray(), serialize(n))
        return n
    }

    override fun addPropertyLocal(key: Long, p: P): P {
        //TODO: close toTimestamp of previous property version
        db.put(properties, "${p.id}".toByteArray(), serialize(p))
        return p
    }

    override fun addEdgeLocal(key: Long, r: R): R {
        //TODO: close toTimestamp of previous edge version
        db.put(edges, "${r.id}".toByteArray(), serialize(r))
        return r
    }

    override fun getProps(): MutableList<P> {
        throw NotImplementedException()
    }

    override fun getNodes(): MutableList<N> {
        val acc: MutableList<N> = mutableListOf()
        val iterator = db.newIterator(nodes)
        iterator.seekToFirst()
        while (iterator.isValid) {
            acc += deserialize<N>(iterator.value(), this)
            iterator.next()
        }
        return acc
    }

    override fun getEdges(): MutableList<R> {
        throw NotImplementedException()
    }

    override fun getProp(id: Int): P {
        val b = db.get(properties, "$id".toByteArray())
        return deserialize<P>(b, this)
    }

    override fun getNode(id: Long): N {
        val b = db.get(nodes, "$id".toByteArray())
        return deserialize<N>(b, this)
    }

    override fun getEdge(id: Int): R {
        val b = db.get(edges, "$id".toByteArray())
        return deserialize<R>(b, this)
    }
}
