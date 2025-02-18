package it.unibo.graph

import it.unibo.graph.structure.CustomGraph
import org.apache.commons.lang3.NotImplementedException
import org.rocksdb.*
import java.io.*

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

val LABEL = "label"

interface Elem: Serializable {
    val id: Number
    val type: String
}

interface IStep {
    val type: String?
    val properties: Pair<String, Any>?
}

class Step(override val type: String? = null, override val properties: Pair<String, Any>? = null) : IStep

fun search(pattern: List<Step?>): MutableList<List<Elem>> {
    val visited: MutableSet<Number> = mutableSetOf()
    val acc: MutableList<List<Elem>> = mutableListOf()
    fun dfs(node: Elem, index: Int, path: List<Elem>) {
        if (pattern[index] == null || (pattern[index]!!.type == null || pattern[index]!!.type == node.type)) {
            val curPath = path + listOf(node)
            if (curPath.size == pattern.size) {
                acc.add(curPath)
                return
            }
            if (visited.contains(node.id)) { return }
            if (index % 2 == 0) { // is node
                visited += node.id
                (node as N).getRels(direction = Direction.OUT, includeHasTs = true).forEach {
                    dfs(it, index + 1, curPath)
                }
            } else { // is edge
                val r = (node as R)
                if (node.type == HAS_TS) {
                    App.tsm.getTS(r.toN).getValues().forEach {
                        dfs(it, index + 1, curPath)
                    }
                } else {
                    dfs(App.g.getNode(r.toN), index + 1, curPath)
                }

            }
            /* node.getRels(direction = Direction.OUT, label = null).forEach {
                dfs(nodes[it.toN], index + 1, curPath)
            } */
        }
    }
    for (node in App.g.getNodes()) {
        if (!visited.contains(node.id)) {
            dfs(node, 0, emptyList())
        }
    }
    return acc
}

interface Graph {
    fun clear()
    fun createNode(label: String, value: Long? = null, id: Long = nextNodeIdOffset(), from: Long, to: Long): N = N(id, label, value = value, fromTimestamp = from, toTimestamp = to)
    fun nextNodeIdOffset(): Long = encodeBitwise(GRAPH_SOURCE, nextNodeId())
    fun nextNodeId(): Long
    fun addNode(label: String, value: Long? = null, from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE): N = addNode(createNode(label, value, from = from, to = to))
    fun addNode(n: N): N
    fun createProperty(nodeId: Long, key: String, value: Any, type: PropType, id: Int = nextPropertyId()): P = P(id, nodeId, key, value, type)
    fun nextPropertyId(): Int
    fun addProperty(nodeId: Long, key: String, value: Any, type: PropType): P = addProperty(createProperty(nodeId, key, value, type))
    fun addProperty(p: P): P
    fun createEdge(label: String, fromNode: Long, toNode: Long, id: Int = nextEdgeId(), from: Long, to: Long): R = R(id, label, fromNode, toNode, fromTimestamp = from, toTimestamp = to)
    fun nextEdgeId(): Int
    fun addEdge(r: R): R
    fun addEdge(label: String, fromNode: Long, toNode: Long, id: Int = nextEdgeId(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE): R = addEdge(createEdge(label, fromNode, toNode, id=id, from, to))
    fun getProps(): MutableList<P>
    fun getNodes(): MutableList<N>
    fun getEdges(): MutableList<R>
    fun getProp(id: Int): P
    fun getNode(id: Long): N
    fun getEdge(id: Int): R
}

fun encodeBitwise(x: Long, y: Long, offset: Int = 44, mask: Long = 0xFFFFFFFFFFF): Long {
    return (x shl offset) or (y and mask)
}

fun decodeBitwise(z: Long, offset: Int = 44, mask: Long = 0xFFFFFFFFFFF): Pair<Long, Long> {
    val x = (z shr offset)
    val y = (z and mask)
    return Pair(x, y)
}

fun decodeBitwiseSource(z: Long, offset: Int = 44): Long {
    return z shr offset
}

open class GraphMemory: Graph {
    private val nodes: MutableList<N> = ArrayList()
    private val rels: MutableList<R> = ArrayList()
    private val props: MutableList<P> = ArrayList()

    override fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
    }

    override fun nextNodeId(): Long = nodes.size.toLong()

    override fun addNode(n: N): N {
        if (n.id >= nodes.size) {
            nodes += n
        } else {
            nodes[(n.id as Number).toInt()] = n
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
            val (tsId, timestamp) = decodeBitwise(r.fromN)
            val ts = App.tsm.getTS(tsId)
            val n = ts.get(timestamp)
            n.relationships += r
            ts.add(n)
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

    override fun getNode(id: Long): N {
        return nodes[(id as Number).toInt()]
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
        if (r.id == DUMMY_ID) {
            val (tsId, timestamp) = decodeBitwise(r.fromN)
            val ts = App.tsm.getTS(tsId)
            val n = ts.get(timestamp)
            n.relationships += r
            ts.add(n)
        } else {
            db.put(edges, "${r.id}".toByteArray(), serialize(r))
        }
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

    override fun getNode(id: Long): N {
        val b = db.get(nodes, "$id".toByteArray())
        return deserialize<N>(b)
    }

    override fun getEdge(id: Int): R {
        val b = db.get(edges, "$id".toByteArray())
        return deserialize<R>(b)
    }
}

object App {
    //    val tsm = MemoryTSManager()
    //    val g: CustomGraph = CustomGraph(GraphMemory())
    val tsm = RocksDBTSM()
    val g: CustomGraph = CustomGraph(GraphRocksDB())
}