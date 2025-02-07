package it.unibo.graph

import it.unibo.graph.structure.CustomGraph
import org.rocksdb.*

interface Graph {
    fun clear()
    fun addNode(label: String, value: Long? = null): N
    fun addProperty(nodeId: Int, key: String, value: Any, type: PropType): P
    fun addRel(label: String, fromNode: Int, toNode: Int): R
    fun addTS(): TS
    fun getProps(): MutableList<P>
    fun getNodes(): MutableList<N>
    fun getEdges(): MutableList<R>
    fun getProp(id: Int): P
    fun getNode(id: Int): N
    fun getEdge(id: Int): R
    fun getTS(id: Int): TS
}

open class GraphMemory : Graph {
    private val nodes: MutableList<N> = ArrayList()
    private val rels: MutableList<R> = ArrayList()
    private val props: MutableList<P> = ArrayList()
    private val ts: MutableList<TS> = ArrayList()

    override fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
        ts.clear()
    }

    override fun addNode(label: String, value: Long?): N {
        val n = N(nodes.size, label, value = value)
        nodes += n
        return n
    }

    override fun addProperty(nodeId: Int, key: String, value: Any, type: PropType): P {
        val p = P(props.size, nodeId, key, value, type)
        props += p
        return p
    }

    override fun addRel(label: String, fromNode: Int, toNode: Int): R {
        val r = R(rels.size, label, fromNode, toNode)
        rels += r
        return r
    }

    override fun addTS(): TS {
        val ts1 = TS(ts.size)
        ts += ts1
        return ts1
    }

    override fun getProps(): MutableList<P> {
        return props
    }

    override fun getNodes(): MutableList<N> {
        return nodes
    }

    override fun getEdges(): MutableList<R> {
        return rels
    }

    override fun getProp(id: Int): P {
        return props[id]
    }

    override fun getNode(id: Int): N {
        return nodes[id]
    }

    override fun getEdge(id: Int): R {
        return rels[id]
    }

    override fun getTS(id: Int): TS {
        return ts[id]
    }
}

class GraphRocksDB : Graph {
    val rocksdbNodes: ColumnFamilyDescriptor
    val rocksdbEdges: ColumnFamilyDescriptor
    val rocksdbProperties: ColumnFamilyDescriptor
    val db: RocksDB

    init {
        val options = DBOptions()
        options.setCreateIfMissing(true)
        options.setCreateMissingColumnFamilies(true)
        rocksdbNodes = ColumnFamilyDescriptor("nodes".toByteArray(), ColumnFamilyOptions())
        rocksdbEdges = ColumnFamilyDescriptor("edges".toByteArray(), ColumnFamilyOptions())
        rocksdbProperties = ColumnFamilyDescriptor("properties".toByteArray(), ColumnFamilyOptions())
        val cfDescriptors = listOf(rocksdbNodes, rocksdbEdges, rocksdbProperties)
        val cfHandles: List<ColumnFamilyHandle> = ArrayList()
        db = RocksDB.open(options, "testdb", cfDescriptors, cfHandles)
    }

    override fun clear() {
        TODO("Not yet implemented")
    }

    override fun addNode(label: String, value: Long?): N {
        TODO("Not yet implemented")
    }

    override fun addProperty(nodeId: Int, key: String, value: Any, type: PropType): P {
        TODO("Not yet implemented")
    }

    override fun addRel(label: String, fromNode: Int, toNode: Int): R {
        TODO("Not yet implemented")
    }

    override fun addTS(): TS {
        TODO("Not yet implemented")
    }

    override fun getProps(): MutableList<P> {
        TODO("Not yet implemented")
    }

    override fun getNodes(): MutableList<N> {
        TODO("Not yet implemented")
    }

    override fun getEdges(): MutableList<R> {
        TODO("Not yet implemented")
    }

    override fun getProp(id: Int): P {
        TODO("Not yet implemented")
    }

    override fun getNode(id: Int): N {
        TODO("Not yet implemented")
    }

    override fun getEdge(id: Int): R {
        TODO("Not yet implemented")
    }

    override fun getTS(id: Int): TS {
        TODO("Not yet implemented")
    }
}

object App {
    val g: Graph = CustomGraph()
}