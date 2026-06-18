package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.*
<<<<<<< HEAD

open class MemoryGraph(
    private val nodes: MutableList<N> = ArrayList(),
    private val rels: MutableList<R> = ArrayList(),
    private val props: MutableList<P> = ArrayList()
) : Graph {
=======
import it.unibo.graph.utils.PATH
import org.rocksdb.*
import java.io.File

open class MemoryGraph(
    private val nodes: MutableList<N> = ArrayList(),
    private val edges: MutableList<R> = ArrayList(),
    private val props: MutableList<P> = ArrayList(),
    override val path: String = PATH + "graph_mem",
    override var dynamicDb: RocksDB? = null
) : Graph {
    val cfHandles: List<ColumnFamilyHandle> = ArrayList()
    val options = DBOptions()
    val cfNames = RocksDB.listColumnFamilies(Options(), path)
    init {
        options.setCreateIfMissing(true)
        options.setCreateMissingColumnFamilies(true)
        val cfDescriptors = listOf(ColumnFamilyDescriptor("default".toByteArray(), ColumnFamilyOptions()))
        val propertyPath = "$path/properties"
        val dir = File(propertyPath)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        dynamicDb = RocksDB.open(options, propertyPath, cfDescriptors, cfHandles)
    }
>>>>>>> feat-tssingletable

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as MemoryGraph
<<<<<<< HEAD
        return nodes == other.nodes && rels == other.rels && props == other.props
=======
        return nodes == other.nodes && edges == other.edges && props == other.props
>>>>>>> feat-tssingletable
    }

    override fun hashCode(): Int {
        var result = nodes.hashCode()
<<<<<<< HEAD
        result = 31 * result + rels.hashCode()
=======
        result = 31 * result + edges.hashCode()
>>>>>>> feat-tssingletable
        result = 31 * result + props.hashCode()
        return result
    }

    override var tsm: TSManager? = null

    override fun clear() {
<<<<<<< HEAD
        listOf(nodes, rels, props).forEach { it.clear() }
=======
        listOf(nodes, edges, props).forEach { it.clear() }
>>>>>>> feat-tssingletable
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

    override fun nextPropertyId(): Long = props.size.toLong()

    override fun addPropertyLocal(key: Long, p: P): P {
        if (p.id >= props.size) {
            props += p
        } else {
            props[p.id.toInt()] = p
        }
        return p
    }

<<<<<<< HEAD
    override fun nextEdgeId(): Long = rels.size.toLong()

    override fun addEdgeLocal(key: Long, r: R): R {
        if (r.id >= rels.size) {
            rels += r
        } else {
            rels[r.id.toInt()] = r
=======
    override fun nextEdgeId(): Long = edges.size.toLong()

    override fun addEdgeLocal(key: Long, r: R): R {
        if (r.id >= edges.size) {
            edges += r
        } else {
            edges[r.id.toInt()] = r
>>>>>>> feat-tssingletable
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
<<<<<<< HEAD
        return rels
=======
        return edges
>>>>>>> feat-tssingletable
    }

    override fun getProp(id: Long): P {
        return props[id.toInt()]
    }

    override fun getNode(id: Long): N {
        return nodes[id.toInt()]
    }

    override fun getEdge(id: Long): R {
<<<<<<< HEAD
        return rels[id.toInt()]
=======
        return edges[id.toInt()]
    }

    override fun close() {
        cfHandles.forEach { it.close() }
        dynamicDb?.closeE()
        options.close()
        File("$path/properties/LOCK").delete()
        tsm?.close()
>>>>>>> feat-tssingletable
    }
}