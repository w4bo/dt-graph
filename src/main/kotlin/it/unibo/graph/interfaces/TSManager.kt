package it.unibo.graph.interfaces

interface TSManager {
    fun addTS(): TS
    fun nextTSId(): Long
    fun getTS(id: Long): TS
    fun clear()
}