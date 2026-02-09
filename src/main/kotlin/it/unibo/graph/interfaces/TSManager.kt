package it.unibo.graph.interfaces

interface TSManager {
    val g: Graph
    fun addTS(id: Long): TS
    fun getTS(id: Long): TS
    fun clear()
}