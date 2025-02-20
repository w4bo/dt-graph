package it.unibo.graph.structure

import it.unibo.graph.App
import it.unibo.graph.HAS_TS
import it.unibo.graph.N
import it.unibo.graph.PropType
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.structure.*

class CustomVertex(
    id: Long,
    type: String,
    value: Long? = null,
    timestamp: Long? = null,
    location: Pair<Double, Double>? = null,
    override val fromTimestamp: Long,
    override var toTimestamp: Long
) : Vertex, N(id, type, value = value, timestamp = timestamp, location = location, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp) {

    override fun id(): Any {
        return id
    }

    override fun label(): String {
        return type
    }

    override fun graph(): Graph {
        return App.g
    }

    override fun <V : Any?> property(
        cardinality: VertexProperty.Cardinality?,
        key: String?,
        value: V,
        vararg keyValues: Any?
    ): VertexProperty<V> {
        return App.g.addProperty(id, key!!, value.toString(), PropType.STRING) as CustomProperty<V>
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
                    CustomProperty(it.id, it.nodeId, it.key, it.value, it.type, fromTimestamp = it.fromTimestamp, toTimestamp = it.toTimestamp)
                }
            }
            .iterator()
    }

    override fun addEdge(label: String, inVertex: Vertex, vararg keyValues: Any?): Edge {
        return App.g.addEdge(label, id, inVertex.id() as Long) as CustomEdge
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
                if (r.type == HAS_TS) {
                    App.tsm.getTS(r.toN).getValues().map { it as CustomVertex }
                } else {
                    listOf(App.g.getNode(if (direction == Direction.IN) r.fromN else r.toN) as CustomVertex)
                }
            }
            .iterator()
    }

    override fun equals(other: Any?): Boolean {
        return super.equals(other)
    }
}