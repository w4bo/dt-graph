package it.unibo.graph.structure

import it.unibo.graph.N
import it.unibo.graph.PropType
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.structure.*

class CustomVertex(id: Int, type: String, val graph: Graph) : Vertex, N(id, type) {

    override fun id(): Any {
        return id
    }

    override fun label(): String {
        return type
    }

    override fun graph(): Graph {
        return graph
    }

    override fun <V : Any?> property(
        cardinality: VertexProperty.Cardinality?,
        key: String?,
        value: V,
        vararg keyValues: Any?
    ): VertexProperty<V> {
        return it.unibo.graph.Graph.addProperty2(id, key!!, value.toString(), PropType.STRING) as CustomProperty<V>
    }

    override fun remove() {
        throw NotImplementedException()
    }

    override fun <V : Any?> properties(vararg propertyKeys: String?): Iterator<VertexProperty<V>> {
        return getProps(filter = null).map { it as CustomProperty<V> }.iterator()
    }

    override fun addEdge(label: String, inVertex: Vertex, vararg keyValues: Any?): Edge {
        return it.unibo.graph.Graph.addRel2(label, id, inVertex.id() as Int, graph) as CustomEdge
    }

    override fun edges(direction: Direction?, vararg edgeLabels: String?): Iterator<Edge> {
        return getRels().map { it as CustomEdge }.iterator()
    }

    override fun vertices(direction: Direction, vararg edgeLabels: String): Iterator<Vertex> {
        return getRels(direction = if (direction == Direction.IN) it.unibo.graph.Direction.IN else it.unibo.graph.Direction.OUT)
            .map { r -> it.unibo.graph.Graph.nodes[if (direction == Direction.IN) r.fromN else r.toN] as CustomVertex }
            .iterator()
    }
}