package it.unibo.graph

import it.unibo.graph.structure.CustomGraph
import org.rocksdb.*
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.Serializable

interface Graph {
    fun clear()
    fun createNode(label: String, value: Long? = null): N = N(nextNodeId(), label, value = value)
    fun nextNodeId(): Int
    fun addNode(label: String, value: Long? = null): N = addNode(createNode(label, value))
    fun addNode(n: N): N
    fun createProperty(nodeId: Int, key: String, value: Any, type: PropType): P = P(nextPropertyId(), nodeId, key, value, type)
    fun nextPropertyId(): Int
    fun addProperty(nodeId: Int, key: String, value: Any, type: PropType): P = addProperty(createProperty(nodeId, key, value, type))
    fun addProperty(p: P): P
    fun createEdge(label: String, fromNode: Int, toNode: Int): R = R(nextEdgeId(), label, fromNode, toNode)
    fun nextEdgeId(): Int
    fun addEdge(r: R): R
    fun addEdge(label: String, fromNode: Int, toNode: Int): R = addEdge(createEdge(label, fromNode, toNode))
    fun addTS(): TS
    fun nextTSId(): Int
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

    override fun nextNodeId(): Int = nodes.size

    override fun addNode(n: N): N {
        nodes += n
        return n
    }

    override fun nextPropertyId(): Int = props.size

    override fun addProperty(p: P): P {
        props += p
        return p
    }

    override fun nextEdgeId(): Int = rels.size

    override fun addEdge(r: R): R {
        rels += r
        return r
    }

    override fun addTS(): TS {
        val ts1 = TS(nextTSId())
        ts += ts1
        return ts1
    }

    override fun nextTSId(): Int = ts.size

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
    val nodes: ColumnFamilyHandle
    val edges: ColumnFamilyHandle
    val properties: ColumnFamilyHandle
    val db: RocksDB
    var nodeId = 0
    var edgeId = 0
    var propId = 0
    var tsId = 0

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
        db = RocksDB.open(options, "testdb", cfDescriptors, cfHandles)
        nodes = cfHandles[1]
        edges = cfHandles[2]
        properties = cfHandles[3]
    }

    override fun clear() {
        TODO("Not yet implemented")
    }

    // Serialize an object to byte array
    fun serialize(obj: Serializable): ByteArray {
        ByteArrayOutputStream().use { bos ->
            ObjectOutputStream(bos).use { out -> out.writeObject(obj) }
            return bos.toByteArray()
        }
    }

    override fun nextNodeId(): Int = nodeId++

    override fun nextPropertyId(): Int = propId++

    override fun nextEdgeId(): Int = edgeId++

    override fun nextTSId(): Int = tsId++

    override fun addNode(n: N): N {
        //db.put(rocksdbNodes, "${n.id}".toByteArray(), serialize(n))
        return n
    }

    override fun addProperty(p: P): P {
        TODO("Not yet implemented")
    }

    override fun addEdge(r: R): R {
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
    val g: CustomGraph = CustomGraph(GraphMemory())
}