package it.unibo.graph.interfaces

interface ElemP: Elem {
    val type: String
    var nextProp: Int?
    val properties: MutableList<P>
    fun getProps(next: Int? = nextProp, filter: PropType? = null, name: String? = null, fromTimestamp: Long = Long.MIN_VALUE, toTimestamp: Long = Long.MAX_VALUE, timeaware: Boolean = true): List<P> {
        fun filter(p: P): Boolean =
            (filter == null || p.type == filter)
                    && (name == null || p.key == name)
                    && p.timeOverlap(timeaware, fromTimestamp, toTimestamp)
        return if (next == null) {
            properties.filter { filter(it) }
        } else {
            val p = g.getProp(next)
            (if (filter(p)) listOf(p) else emptyList()) + getProps(p.next, filter, name, fromTimestamp, toTimestamp, timeaware)
        }
    }
}