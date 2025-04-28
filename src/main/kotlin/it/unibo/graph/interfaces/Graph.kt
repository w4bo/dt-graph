package it.unibo.graph.interfaces

import it.unibo.graph.structure.CustomProperty
import it.unibo.graph.utils.*
import org.apache.tinkerpop.gremlin.structure.PropertyType
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.geojson.GeoJsonReader

interface Graph {
    var tsm: TSManager?
    fun getTSM(): TSManager = tsm!!
    fun close() {}
    fun clear()
    fun createNode(label: Label, value: Long? = null, id: Long = nextNodeIdOffset(), from: Long, to: Long): N = N(id, label, value = value, fromTimestamp = from, toTimestamp = to, g = this)
    fun nextNodeIdOffset(): Long = encodeBitwise(GRAPH_SOURCE, nextNodeId())
    fun nextNodeId(): Long
    fun addNode(label: Label, value: Long? = null, from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE): N = addNode(createNode(label, value, from = from, to = to))
    fun addNode(n: N): N
    fun createProperty(sourceId: Long, sourceType: Boolean, key: String, value: Any, type: PropType, id: Int = nextPropertyId(), from: Long, to: Long): P =
        P(id, sourceId, sourceType, key, value, type, fromTimestamp = from, toTimestamp = to, g = this)
    fun nextPropertyId(): Int
    fun addProperty(sourceId: Long, key: String, value: Any, type: PropType, from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, sourceType: Boolean = NODE, id: Int = nextPropertyId()): P  {
        if(type == PropType.GEOMETRY){
            return addProperty(createProperty(sourceId, sourceType, key,  GeoJsonReader().read(value as String), type, from = from, to = to, id = id))
        }else{
            return addProperty(createProperty(sourceId, sourceType, key, value, type, from = from, to = to, id = id))
        }

    }

    fun addProperty(p: P): P {
        val (source, key) = decodeBitwise(p.sourceId)
        return if (source == GRAPH_SOURCE) {
            addPropertyLocal(key, p)
        } else {
            addPropertyTS(source, key, p)
        }
    }

    fun addPropertyLocal(key: Long, p: P): P
    fun addPropertyTS(tsId: Long, key: Long, p: P): P {
        if (p.sourceType == NODE) {
            val ts = tsm!!.getTS(tsId)
            val n = ts.get(key)
            assert(n.fromTimestamp >= p.fromTimestamp && p.toTimestamp <= n.toTimestamp) { p.toString() }
            n.properties += p
            ts.add(n, isUpdate = true)
            return p
        } else {
            throw IllegalArgumentException("Cannot add property to edge in TS")
        }
    }

    fun createEdge(label: Label, fromNode: Long, toNode: Long, id: Int = nextEdgeId(), from: Long, to: Long): R = R(id, label, fromNode, toNode, fromTimestamp = from, toTimestamp = to, g = this)
    fun nextEdgeId(): Int
    fun addEdge(r: R): R {
        val (source, key) = decodeBitwise(r.fromN)
        return if (source == GRAPH_SOURCE) {
            addEdgeLocal(key, r)
        } else {
            addEdgeTS(source, key, r)
        }
    }

    fun addEdgeLocal(key: Long, r: R): R
    fun addEdgeTS(tsId: Long, key: Long, r: R): R {
        val ts = tsm!!.getTS(tsId)
        val n = ts.get(key)
        n.relationships += r
        ts.add(n, isUpdate = true)
        return r
    }

    fun addEdge(label: Label, fromNode: Long, toNode: Long, id: Int = nextEdgeId(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE): R = addEdge(createEdge(label, fromNode, toNode, id=id, from, to))
    fun getProps(): MutableList<P>
    fun getNodes(): MutableList<N>
    fun getEdges(): MutableList<R>
    fun getProp(id: Int): P
    fun getNode(id: Long): N
    fun getEdge(id: Int): R
}
