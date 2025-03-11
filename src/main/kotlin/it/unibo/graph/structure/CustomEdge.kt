package it.unibo.graph.structure

import it.unibo.graph.App
import it.unibo.graph.interfaces.R
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.structure.*

class CustomEdge(
    id: Int, type: String, prevNode: Long, nextNode: Long, override val fromTimestamp: Long,
    override var toTimestamp: Long
) : Edge, R(id, type, prevNode, nextNode, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp) {
    override fun id(): Any {
        return id
    }

    override fun label(): String {
        return type
    }

    override fun graph(): Graph {
        return App.g
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
        return listOf(App.g.getNode(fromN) as Vertex, App.g.getNode(toN) as Vertex).iterator()
    }

    override fun equals(other: Any?): Boolean {
        return super.equals(other)
    }
}