class R(val id: Int, val type: String, val prevNode: Int, val nextNode: Int, var next: Int? = null) {
    init {
        listOf(Graph.nodes[prevNode], Graph.nodes[nextNode]).forEach {
            if (it.nextRel == null) {
                it.nextRel = id
            } else {
                next = it.nextRel
                it.nextRel = id
            }
        }
    }

    override fun toString(): String {
        return "($prevNode)-[id: $id, type: $type}->($nextNode)"
    }
}