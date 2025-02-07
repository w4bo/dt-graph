package it.unibo.graph

import it.unibo.graph.structure.CustomGraph
import org.apache.commons.lang3.NotImplementedException
import org.rocksdb.*
import java.io.*
import kotlin.reflect.KClass

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

fun <T : TS> makeTS(type: KClass<T>, id: Int): TS {
    return when (type::class) {
        MemoryTS::class -> CustomTS(MemoryTS(id))
        else -> throw IllegalArgumentException()
    }
}

open class GraphMemory() : Graph { // private val tsType: KClass<T>
    private val nodes: MutableList<N> = ArrayList()
    private val rels: MutableList<R> = ArrayList()
    private val props: MutableList<P> = ArrayList()
    private val tss: MutableList<TS> = ArrayList()

    override fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
        tss.clear()
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
        rels += r
        return r
    }

    override fun addTS(): TS {
        val ts = CustomTS(MemoryTS(nextTSId()))
        tss += ts
        return ts
    }

    override fun nextTSId(): Int = tss.size

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
        return tss[id]
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
    val DB_NAME = "testdb"
    private val tss: MutableList<TS> = ArrayList()

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
        // tss.clear()
        File(DB_NAME).deleteRecursively()
    }

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

    override fun nextNodeId(): Int = nodeId++

    override fun nextPropertyId(): Int = propId++

    override fun nextEdgeId(): Int = edgeId++

    override fun nextTSId(): Int = tsId++

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

    override fun addTS(): TS {
        val ts = CustomTS(MemoryTS(nextTSId()))
        tss += ts
        return ts
    }

    override fun getProps(): MutableList<P> {
        throw NotImplementedException()
    }

    override fun getNodes(): MutableList<N> {
        var acc: MutableList<N> = mutableListOf()
        val iterator = db.newIterator(nodes) // Iterate and filter entries for "users"
        // iterator.seek("users:".toByteArray()) // Start at "users:"
        iterator.seekToFirst()
        while (iterator.isValid) {
            // val key = String(iterator.key())
            // if (!key.startsWith("users:")) break // Stop if outside "users" prefix
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

    override fun getTS(id: Int): TS = tss[id]
}

object App {
//     val g: CustomGraph = CustomGraph(GraphMemory())
    val g: CustomGraph = CustomGraph(GraphRocksDB())
}