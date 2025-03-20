package it.unibo.graph.structure

import it.unibo.graph.interfaces.Label
import it.unibo.graph.interfaces.R
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.structure.*

class CustomEdge(
    id: Int,
    type: Label,
    prevNode: Long,
    nextNode: Long,
    fromTimestamp: Long,
    toTimestamp: Long,
    g: it.unibo.graph.interfaces.Graph
) : Edge, R(id, type, prevNode, nextNode, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g) {
    override fun id(): Any {
        return id
    }

    override fun label(): String {
        return type.toString()
    }

    override fun graph(): Graph {
        return g as Graph
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
        return listOf(g.getNode(fromN) as Vertex, g.getNode(toN) as Vertex).iterator()
    }

    override fun equals(other: Any?): Boolean {
        return super.equals(other)
    }
}