package it.unibo.graph.interfaces

<<<<<<< HEAD
interface TSManager {
    val g: Graph
    fun addTS(id: Long): TS
    fun getTS(id: Long): TS
    fun clear()
=======
enum class TsMode { WRITE, READ }
interface TSManager {
    val g: Graph
    fun addTS(id: Long): TS
    fun getTS(id: Long, mode: TsMode = TsMode.WRITE): TS
    fun clear()
    fun close() {}
>>>>>>> feat-tssingletable
}