package it.unibo.graph.structure

import it.unibo.graph.Graph
import it.unibo.graph.P
import it.unibo.graph.PropType
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.apache.commons.lang3.NotImplementedException

class CustomProperty<V>(id: Int, node: Int, key: String, value: Any, type: PropType) : VertexProperty<V>,
    P(id, node, key, value, type) {

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
}