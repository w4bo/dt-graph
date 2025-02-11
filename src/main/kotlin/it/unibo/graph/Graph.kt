package it.unibo.graph

import it.unibo.graph.structure.CustomGraph
import org.apache.commons.lang3.NotImplementedException
import org.rocksdb.*
import java.io.*
import kotlin.reflect.KClass

// Serialize an object to byte array
fun serialize(obj: Serializable): ByteArray {
    ByteArrayOutputStream().use { bos ->
        ObjectOutputStream(bos).use { out -> out.writeObject(obj) }
        return bos.toByteArray()
    }
}

// Deserialize a byte array to an object
inline fun <reified T : Serializable> deserialize(bytes: ByteArray?): T {
    ByteArrayInputStream(bytes).use { bis ->
        ObjectInputStream(bis).use { `in` -> return `in`.readObject() as T }
    }
}

interface Graph {
    fun clear()
    fun createNode(label: String, value: Long? = null, id: Int = nextNodeId()): N = N(id, label, value = value)
    fun nextNodeId(): Int
    fun addNode(label: String, value: Long? = null): N = addNode(createNode(label, value))
    fun addNode(n: N): N
    fun createProperty(nodeId: Int, key: String, value: Any, type: PropType, id: Int = nextPropertyId()): P = P(id, nodeId, key, value, type)
    fun nextPropertyId(): Int
    fun addProperty(nodeId: Int, key: String, value: Any, type: PropType): P = addProperty(createProperty(nodeId, key, value, type))
    fun addProperty(p: P): P
    fun createEdge(label: String, fromNode: Int, toNode: Int, id: Int = nextEdgeId()): R = R(id, label, fromNode, toNode)
    fun nextEdgeId(): Int
    fun addEdge(r: R): R
    fun addEdge(label: String, fromNode: Int, toNode: Int, id: Int = nextEdgeId()): R = addEdge(createEdge(label, fromNode, toNode, id=id))
    fun getProps(): MutableList<P>
    fun getNodes(): MutableList<N>
    fun getEdges(): MutableList<R>
    fun getProp(id: Int): P
    fun getNode(id: Int): N
    fun getEdge(id: Int): R
}

open class GraphMemory: Graph { // private val tsType: KClass<T>
    private val nodes: MutableList<N> = ArrayList()
    private val rels: MutableList<R> = ArrayList()
    private val props: MutableList<P> = ArrayList()

    override fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
    }

    override fun nextNodeId(): Int = nodes.size

    override fun addNode(n: N): N {
        if (n.id >= nodes.size) {
            nodes += n
        } else {
            nodes[n.id] = n
        }
        return n
    }

    override fun nextPropertyId(): Int = props.size

    override fun addProperty(p: P): P {
        props += p
        return p
    }

    override fun nextEdgeId(): Int = rels.size

    override fun addEdge(r: R): R {
        if (r.id == DUMMY_ID) {
            throw NotImplementedException()
            // App.tsm.getTS(r.fromN).???
        } else {
            rels += r
        }
        return r
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
}

class GraphRocksDB : Graph {
    val nodes: ColumnFamilyHandle
    val edges: ColumnFamilyHandle
    val properties: ColumnFamilyHandle
    val db: RocksDB
    var nodeId = 0
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

    override fun nextNodeId(): Int = nodeId++

    override fun nextPropertyId(): Int = propId++

    override fun nextEdgeId(): Int = edgeId++

    override fun addNode(n: N): N {
        db.put(nodes, "${n.id}".toByteArray(), serialize(n))
        return n
    }

    override fun addProperty(p: P): P {
        db.put(properties, "${p.id}".toByteArray(), serialize(p))
        return p
    }

    override fun addEdge(r: R): R {
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
            acc += deserialize<N>(iterator.value())
            iterator.next()
        }
        return acc
    }

    override fun getEdges(): MutableList<R> {
        throw NotImplementedException()
    }

    override fun getProp(id: Int): P {
        val b = db.get(properties, "$id".toByteArray())
        return deserialize<P>(b)
    }

    override fun getNode(id: Int): N {
        val b = db.get(nodes, "$id".toByteArray())
        return deserialize<N>(b)
    }

    override fun getEdge(id: Int): R {
        val b = db.get(edges, "$id".toByteArray())
        return deserialize<R>(b)
    }
}

object App {
    val tsm = MemoryTSManager()
    val g: CustomGraph = CustomGraph(GraphMemory())
//    val tsm = RocksDBTSM()
//    val g: CustomGraph = CustomGraph(GraphRocksDB())
}