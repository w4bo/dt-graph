package it.unibo.graph

enum class Direction { IN, OUT }

open class R(
    override val id: Int, // id of the relationship
    override val type: String, // label
    val fromN: Long, // from node
    val toN: Long, // to node
    var fromNextRel: Int? = null, // pointer to the next relationship of the `from node`
    var toNextRel: Int? = null, // pointer to the next relationship of the `to node`
    override val fromTimestamp: Long = Long.MIN_VALUE,
    override var toTimestamp: Long = Long.MAX_VALUE,
    override var nextProp: Int? = null,
    override val properties: MutableList<P> = mutableListOf()
): ElemP {
    init {
        if (id != DUMMY_ID) {
            val from = App.g.getNode(fromN)
            val to = App.g.getNode(toN)

            if (from.nextRel == null) { // this is the first edge
                from.nextRel = id
            } else {
                for (it in from.getRels(direction = Direction.OUT, label = type)) {
                    if (it.toN == toN && toTimestamp == Long.MAX_VALUE) {
                        it.toTimestamp = fromTimestamp
                        break
                    }
                }
                fromNextRel = from.nextRel
                from.nextRel = id
            }

            if (to.nextRel == null) { // this is the first edge
                to.nextRel = id
            } else {
                // Non credo mi serva, è la  stessa relazione sopra
                // for (it in to.getRels(direction = Direction.IN, label = type)) {
                //     if (it.fromN == fromN && toTimestamp == Long.MAX_VALUE) {
                //         it.toTimestamp = fromTimestamp
                //         break
                //     }
                // }
                toNextRel = to.nextRel
                to.nextRel = id
            }

            App.g.addNode(from)
            App.g.addNode(to)
        }
    }

    override fun toString(): String {
        return "($fromN)-[id: $id, type: $type, from: $fromTimestamp, to: $toTimestamp}->($toN)"
    }

    override fun equals(other: Any?): Boolean {
        if (other !is R) return false
        return id == other.id
    }
}