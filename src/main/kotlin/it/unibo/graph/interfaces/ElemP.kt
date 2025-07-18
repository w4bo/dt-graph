package it.unibo.graph.interfaces

interface ElemP : Elem {
    val label: Label
    var nextProp: Int?
    val properties: MutableList<P>
    fun getProps(
        next: Int? = nextProp,
        filter: PropType? = null,
        name: String? = null,
        fromTimestamp: Long = Long.MIN_VALUE,
        toTimestamp: Long = Long.MAX_VALUE,
        timeaware: Boolean = true
    ): List<P> {
        val result = mutableListOf<P>()
        val filterCheck: (P) -> Boolean = { p -> (filter == null || p.type == filter) && (name == null || p.key == name) && p.timeOverlap(timeaware, fromTimestamp, toTimestamp) }
        var current = next
        while (current != null) {
            val p = g.getProp(current)
//            if (name != null && name == p.key && timeaware && p.toTimestamp < fromTimestamp) break
            if (filterCheck(p)) result.add(p)
            current = p.next
        }
        if (next == null && properties != null) {
            result.addAll(properties.filter(filterCheck))
        }
        return result
    }
}