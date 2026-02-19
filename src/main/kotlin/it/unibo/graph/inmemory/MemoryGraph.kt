package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.*

open class MemoryGraph(
    private val nodes: MutableList<N> = ArrayList(),
    private val edges: MutableList<R> = ArrayList(),
    private val props: MutableList<P> = ArrayList()
) : Graph {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as MemoryGraph
        return nodes == other.nodes && edges == other.edges && props == other.props
    }

    override fun hashCode(): Int {
        var result = nodes.hashCode()
        result = 31 * result + edges.hashCode()
        result = 31 * result + props.hashCode()
        return result
    }

    override var tsm: TSManager? = null

    override fun clear() {
        listOf(nodes, edges, props).forEach { it.clear() }
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

    override fun nextEdgeId(): Long = edges.size.toLong()

    override fun addEdgeLocal(key: Long, r: R): R {
        if (r.id >= edges.size) {
            edges += r
        } else {
            edges[r.id.toInt()] = r
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
        return edges
    }

    override fun getProp(id: Long): P {
        return props[id.toInt()]
    }

    override fun getNode(id: Long): N {
        return nodes[id.toInt()]
    }

    override fun getEdge(id: Long): R {
        return edges[id.toInt()]
    }
}