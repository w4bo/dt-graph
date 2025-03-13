package it.unibo.graph.structure

import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.utils.HAS_TS
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.structure.*

class CustomVertex(
    id: Long,
    type: String,
    value: Long? = null,
    timestamp: Long? = null,
    location: String? = null,
    fromTimestamp: Long,
    toTimestamp: Long,
    g: it.unibo.graph.interfaces.Graph
) : Vertex, N(id, type, value = value, timestamp = timestamp, location = location, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g) {

    override fun id(): Any {
        return id
    }

    override fun label(): String {
        return type
    }

    override fun graph(): Graph {
        return g as Graph
    }

    override fun <V : Any?> property(
        cardinality: VertexProperty.Cardinality?,
        key: String?,
        value: V,
        vararg keyValues: Any?
    ): VertexProperty<V> {
        return g.addProperty(id, key!!, value.toString(), PropType.STRING) as CustomProperty<V>
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
                    CustomProperty(
                        it.id,
                        it.sourceId,
                        it.sourceType,
                        it.key,
                        it.value,
                        it.type,
                        fromTimestamp = it.fromTimestamp,
                        toTimestamp = it.toTimestamp,
                        g = g
                    )
                }
            }
            .iterator()
    }

    override fun addEdge(label: String, inVertex: Vertex, vararg keyValues: Any?): Edge {
        return g.addEdge(label, id, inVertex.id() as Long) as CustomEdge
    }

    fun dir2dir(direction: Direction): it.unibo.graph.interfaces.Direction {
        return if (direction == Direction.IN) {
            it.unibo.graph.interfaces.Direction.IN
        } else {
            it.unibo.graph.interfaces.Direction.OUT
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
                if (r.type == HAS_TS) {
                    g.getTSM().getTS(r.toN).getValues().map { it as CustomVertex }
                } else {
                    listOf(g.getNode(if (direction == Direction.IN) r.fromN else r.toN) as CustomVertex)
                }
            }
            .iterator()
    }

    override fun equals(other: Any?): Boolean {
        return super.equals(other)
    }
}