class N(val id: Int, val type: String, var nextRel: Int? = null, var nextProp: Int? = null) {
    fun getProps(next: Int? = nextProp): List<P> {
        return if (next == null) {
            listOf()
        } else {
            val p = Graph.props[next]
            listOf(p) + getProps(p.next)
        }
    }

    fun findProp(propType: PropType, next: Int? = nextProp): P? {
        return if (next == null) {
            null
        } else {
            val p = Graph.props[next]
            if (p.type == propType) p else findProp(propType, p.next)
        }
    }

    fun getTS(): List<TSelem> {
        val ts = findProp(PropType.TS)
        if (ts != null) {
            val ts = Graph.ts[(ts.value as Number).toInt()]
            fun getTSelems(next: Int? = 0): List<TSelem> {
                return if (next == null) {
                    listOf()
                } else {
                    val p = ts.values[next]
                    listOf(p) + getTSelems(p.next)
                }
            }
            return getTSelems()
        } else {
            return listOf()
        }
    }

    fun getTSByType() {

    }

    fun getRels(next: Int? = nextRel): List<R> {
        return if (next == null) {
            listOf<R>()
        } else {
            val p = Graph.rels[next]
            listOf(p) + getRels(p.next)
        }
    }

    override fun toString(): String {
        return "(id: $id, type: $type)"
    }
}