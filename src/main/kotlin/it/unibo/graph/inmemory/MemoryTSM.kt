package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
<<<<<<< HEAD
=======
import it.unibo.graph.interfaces.TsMode
>>>>>>> feat-tssingletable

class MemoryTSM(override val g: Graph): TSManager {
    private val tss: MutableMap<Long, TS> = mutableMapOf()

<<<<<<< HEAD
    override fun getTS(id: Long): TS {
=======
    override fun getTS(id: Long, mode: TsMode): TS {
>>>>>>> feat-tssingletable
        return tss[id]!!
    }

    override fun addTS(id: Long): TS {
        val cId = id + 1
        val ts = MemoryTS(g, cId)
        tss[cId] = ts
        return ts
    }

    override fun clear() {
        tss.clear()
    }
}
