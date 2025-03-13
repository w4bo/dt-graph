package it.unibo.graph.interfaces

interface TSManager {
    val g: Graph
    fun addTS(): TS
    fun nextTSId(): Long
    fun getTS(id: Long): TS
    fun clear()
}