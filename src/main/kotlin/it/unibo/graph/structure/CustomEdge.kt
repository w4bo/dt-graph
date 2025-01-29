package it.unibo.graph.structure

import it.unibo.graph.R
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.commons.lang3.NotImplementedException
class CustomEdge(id: Int, type: String, prevNode: Int, nextNode: Int, val graph: Graph): Edge, R(id, type, prevNode, nextNode) {
    override fun id(): Any {
        return id
    }

    override fun label(): String {
        return type
    }

    override fun graph(): Graph {
        return graph
    }

    override fun <V : Any?> property(key: String?, value: V): Property<V> {
        throw NotImplementedException()
    }

    override fun remove() {
        throw NotImplementedException()
    }

    override fun <V : Any?> properties(vararg propertyKeys: String?): MutableIterator<Property<V>> {
        throw NotImplementedException()
    }

    override fun vertices(direction: Direction?): Iterator<Vertex> {
        return listOf(it.unibo.graph.Graph.nodes[prevNode] as Vertex, it.unibo.graph.Graph.nodes[nextNode] as Vertex).iterator()
    }
}