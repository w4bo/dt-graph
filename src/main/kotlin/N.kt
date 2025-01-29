class N(val id: Int, val type: String, var nextRel: Int? = null, var nextProp: Int? = null) {
    fun getProps(next: Int? = nextProp, filter: PropType? = null): List<P> {
        return if (next == null) {
            listOf()
        } else {
            val p = Graph.props[next]
            (if (filter == null || p.type == filter) listOf(p) else listOf()) + getProps(p.next, filter)
        }
    }

    fun getTS(): List<Pair<P, List<TSelem>>> {
        return getProps(filter = PropType.TS).map { p ->
            val ts = Graph.ts[(p.value as Number).toInt()]
            fun getTSelems(next: Int? = 0): List<TSelem> {
                return if (next == null) {
                    listOf()
                } else {
                    val pNext = ts.values[next]
                    listOf(pNext) + getTSelems(pNext.next)
                }
            }
            Pair(p, getTSelems())
        }
    }

    fun getRels(next: Int? = nextRel): List<R> {
        return if (next == null) {
            listOf()
        } else {
            val p = Graph.rels[next]
            listOf(p) + getRels(p.next)
        }
    }

    override fun toString(): String {
        return "(id: $id, type: $type)"
    }
}