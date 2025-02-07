package it.unibo.graph

import it.unibo.graph.structure.CustomEdge
import it.unibo.graph.structure.CustomProperty
import it.unibo.graph.structure.CustomVertex

object Graph {
    val nodes: MutableList<N> = ArrayList()
    val rels: MutableList<R> = ArrayList()
    val props: MutableList<P> = ArrayList()
    val ts: MutableList<TS> = ArrayList()

    fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
        ts.clear()
    }

    fun addNode(label: String, value: Long? = null): N {
        val n = N(nodes.size, label, value=value)
        nodes += n
        return n
    }

    fun addProperty(nodeId: Int, key: String, value: Any, type: PropType): P {
        val p = P(props.size, nodeId, key, value, type)
        props += p
        return p
    }

    fun addRel(label: String, fromNode: Int, toNode: Int): R {
        val r = R(rels.size, label, fromNode, toNode)
        rels += r
        return r
    }

    fun addNode2(label: String, graph: org.apache.tinkerpop.gremlin.structure.Graph, value: Long?=null): N {
        val n = CustomVertex(nodes.size, label, graph, value=value)
        nodes += n
        return n
    }

    fun addProperty2(nodeId: Int, key: String, value: Any, type: PropType): P {
        val p = CustomProperty<String>(props.size, nodeId, key, value, type)
        props += p
        return p
    }

    fun addRel2(label: String, fromNode: Int, toNode: Int, graph: org.apache.tinkerpop.gremlin.structure.Graph): R {
        val r = CustomEdge(rels.size, label, fromNode, toNode, graph)
        rels += r
        return r
    }
}