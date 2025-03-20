package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.*
import it.unibo.graph.utils.DUMMY_ID

open class MemoryGraph: Graph {
    override var tsm: TSManager? = null
    private val nodes: MutableList<N> = ArrayList()
    private val rels: MutableList<R> = ArrayList()
    private val props: MutableList<P> = ArrayList()

    override fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
    }

    override fun nextNodeId(): Long = nodes.size.toLong()

    override fun addEdge(r: R): R {
        if (r.id >= rels.size || r.id == DUMMY_ID) {
            return super.addEdge(r)
        } else {
            rels[(r.id as Number).toInt()] = r
            return r
        }
    }

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
        rels += r
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