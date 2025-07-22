package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.*

open class MemoryGraph(
    private val nodes: MutableList<N> = ArrayList(),
    private val rels: MutableList<R> = ArrayList(),
    private val props: MutableList<P> = ArrayList()
) : Graph {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as MemoryGraph
        return nodes == other.nodes && rels == other.rels && props == other.props
    }

    override fun hashCode(): Int {
        var result = nodes.hashCode()
        result = 31 * result + rels.hashCode()
        result = 31 * result + props.hashCode()
        return result
    }

    override var tsm: TSManager? = null

    override fun clear() {
        listOf(nodes, rels, props).forEach { it.clear() }
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

    override fun addPropertyLocal(key: Long, p: P): P {
        if (p.id >= props.size) {
            props += p
        } else {
            props[p.id] = p
        }
        return p
    }

    override fun nextEdgeId(): Int = rels.size

    override fun addEdgeLocal(key: Long, r: R): R {
        if (r.id >= rels.size) {
            rels += r
        } else {
            rels[(r.id as Number).toInt()] = r
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