package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.*
import org.locationtech.jts.io.geojson.GeoJsonReader

open class GraphMemory: Graph {
    private val nodes: MutableList<N> = ArrayList()
    private val rels: MutableList<R> = ArrayList()
    private val props: MutableList<P> = ArrayList()

    override fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
    }

    override fun nextNodeId(): Long = nodes.size.toLong()

    override fun addEdge(r: R): R {
        if (r.id >= rels.size || r.id == DUMMY_ID) {
            return super.addEdge(r)
        } else {
            rels[(r.id as Number).toInt()] = r
            return r
        }
    }

    override fun addNode(n: N): N {
        if (n.id >= nodes.size) {
            nodes += n
        } else {
            nodes[(n.id as Number).toInt()] = n
        }
        return n
    }

    override fun nextPropertyId(): Int = props.size

    override fun upsertFirstCitizenProperty(prop: P): P? {
        val nodeId = prop.sourceId
        nodes[nodeId.toInt()].location = GeoJsonReader().read(prop.value.toString())
        nodes[nodeId.toInt()].locationTimestamp = prop.fromTimestamp
        return prop
    }

    override fun addPropertyLocal(key: Long, p: P): P {
        // Find the last properties and update its toTimestamp
        // Update only if toTimestamp of old property is > than fromTimestamp of new property.
        props.filter { it.key == p.key && it.sourceId == p.sourceId }
            .maxByOrNull { it.toTimestamp }
            ?.let {
                if (it.toTimestamp > p.fromTimestamp) {
                    it.toTimestamp = p.fromTimestamp
                }
            }
        props += p
        return p
    }

    override fun nextEdgeId(): Int = rels.size

    override fun addEdgeLocal(key: Long, r: R): R {
        // Find the last edge version and update its toTimestamp
        // Update only if toTimestamp of old edge is > than fromTimestamp of new property.
        rels.filter { it.id == r.id}
            .maxByOrNull { it.toTimestamp }
            ?.let {
                if (it.toTimestamp > r.fromTimestamp) {
                    it.toTimestamp = r.fromTimestamp
                }
            }
        rels += r
        return r
    }

    override fun getProps(): MutableList<P> {
        return props
    }

    override fun getNodes(): MutableList<N> {
        return nodes
    }

    override fun getEdges(): MutableList<R> {
        return rels
    }

    override fun getProp(id: Int): P {
        return props[id]
    }

    override fun getNode(id: Long): N {
        return nodes[(id as Number).toInt()]
    }

    override fun getEdge(id: Int): R {
        return rels[id]
    }
}