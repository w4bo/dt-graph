package it.unibo.graph.structure

import it.unibo.graph.N
import it.unibo.graph.PropType
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.structure.*

class CustomVertex(
    id: Int,
    type: String,
    val graph: Graph,
    value: Long? = null,
    timestamp: Long? = null,
    location: Pair<Double, Double>? = null
) : Vertex, N(id, type, value = value, timestamp = timestamp, location = location) {

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
        val key = if (propertyKeys.isEmpty()) { null } else { propertyKeys[0] }
        return getProps(name = key)
            .map {
                if (it is CustomProperty<*>) {
                    it as CustomProperty<V>
                } else {
                    CustomProperty(it.id, it.node, it.key, it.value, it.type)
                }
            }
            .iterator()
    }

    override fun addEdge(label: String, inVertex: Vertex, vararg keyValues: Any?): Edge {
        return it.unibo.graph.Graph.addRel2(label, id, inVertex.id() as Int, graph) as CustomEdge
    }

    fun dir2dir(direction: Direction): it.unibo.graph.Direction {
        return if (direction == Direction.IN) {
            it.unibo.graph.Direction.IN
        } else {
            it.unibo.graph.Direction.OUT
        }
    }

    fun getFirst(edgeLabels: Array<out String>): String? {
        return if (edgeLabels.isEmpty()) {
            null
        } else {
            edgeLabels[0]
        }
    }

    override fun edges(direction: Direction, vararg edgeLabels: String): Iterator<Edge> {
        return getRels(direction = dir2dir(direction), label = getFirst(edgeLabels))
            .map { it as CustomEdge }
            .iterator()
    }

    override fun vertices(direction: Direction, vararg edgeLabels: String): Iterator<Vertex> {
        return getRels(direction = dir2dir(direction), label = getFirst(edgeLabels))
            .flatMap { r ->
                if (r.type == "hasTS") {
                    it.unibo.graph.Graph.ts[r.toN].values.map { it as CustomVertex }
                } else {
                    listOf(it.unibo.graph.Graph.nodes[if (direction == Direction.IN) r.fromN else r.toN] as CustomVertex)
                }
            }
            .iterator()
    }
}