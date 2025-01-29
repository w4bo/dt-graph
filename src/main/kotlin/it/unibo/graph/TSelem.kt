package it.unibo.graph

class TSelem(val id: Int, tsId: Int, val timestamp: Long, val value: Int, val location: Pair<Double, Double>? = null, var next: Int? = null) {
    init {
        val ts: TS = Graph.ts[tsId]
        if (ts.values.size > 0) {
            val elem: TSelem = ts.values[ts.values.size - 1]
            next = elem.next
            elem.next = id
        }
    }

    override fun toString(): String {
        return "<id: $id, timestamp: $timestamp, value: $value>"
    }
}