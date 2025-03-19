package it.unibo.graph.interfaces

import it.unibo.graph.utils.*
import org.locationtech.jts.io.geojson.GeoJsonReader

enum class PropType { INT, DOUBLE, STRING, TS, GEOMETRY }

open class P(
    final override val id: Int,
    val sourceId: Long,
    val sourceType: Boolean,
    val key: String,
    val value: Any,
    val type: PropType,
    var next: Int? = null,
    final override val fromTimestamp: Long = Long.MIN_VALUE,
    final override var toTimestamp: Long = Long.MAX_VALUE,
    @Transient final override var g: Graph
) : Elem {
    init {
        if (decodeBitwiseSource(sourceId) == GRAPH_SOURCE && id != DUMMY_ID) { // If the source is the graph and the property is not created at runtime
            val elem: ElemP = if (sourceType == NODE) g.getNode(sourceId) else g.getEdge(sourceId.toInt()) // get the source of the property (either an edge or a node)
            if (elem.nextProp == null) elem.nextProp = id // if the element already has no properties, do nothing
            else {
                // TODO we only update the `toTimestamp` of properties of elements of the graph (and not of the TS)
                // TODO here we iterate all the properties of the element, but maybe we could exploit some sorting over time
                val oldP: P? = elem.getProps(name = key).filter { it.toTimestamp > fromTimestamp }.maxByOrNull { it.toTimestamp } // check if the element contains a property with the same name
                if (oldP != null) { // if so, and if the `toTimestamp` is ge than `fromTimestamp`
                    oldP.toTimestamp = fromTimestamp // update the previous `toTimestamp`
                    g.addProperty(oldP) // update the property
                }
                next = elem.nextProp  // update the next pointer of the node
                elem.nextProp = id
            }
            if (elem is N && key == LOCATION) {
                // TODO why don't edges have the location as a first citizen?
                elem.location = GeoJsonReader().read(value.toString())
                elem.locationTimestamp = fromTimestamp
            }
            if (sourceType == NODE) g.addNode(elem as N) else g.addEdge(elem as R) // store the element again
        }
    }

    override fun toString(): String {
        return "{id: $id, node: $sourceId, key: $key, value: $value, type: $type, from: $fromTimestamp, to: $toTimestamp}"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is P) return false
        return id == other.id
    }
}