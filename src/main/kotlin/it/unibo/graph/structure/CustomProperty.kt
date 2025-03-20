package it.unibo.graph.structure

import it.unibo.graph.interfaces.P
import it.unibo.graph.interfaces.PropType
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty

class CustomProperty<V>(
    id: Int,
    sourceId: Long,
    sourceType: Boolean,
    key: String,
    value: Any,
    type: PropType,
    fromTimestamp: Long,
    toTimestamp: Long,
    g: it.unibo.graph.interfaces.Graph
) : VertexProperty<V>,
    P(id, sourceId, sourceType, key, value, type, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g) {

    override fun key(): String {
        return key
    }

    override fun value(): V {
        return value as V
    }

    override fun isPresent(): Boolean {
        return true;
    }

    override fun element(): Vertex {
        throw NotImplementedException()
    }

    override fun remove() {
        throw NotImplementedException()
    }

    override fun id(): Any {
        return id
    }

    override fun <V : Any?> property(key: String?, value: V): Property<V> {
        throw NotImplementedException()
    }

    override fun <U : Any?> properties(vararg propertyKeys: String?): MutableIterator<Property<U>> {
        throw NotImplementedException()
    }

    override fun equals(other: Any?): Boolean {
        return super.equals(other)
    }
}