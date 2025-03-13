package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
import it.unibo.graph.structure.CustomTS

class MemoryTSM(override val g: Graph): TSManager {
    private val tss: MutableList<TS> = ArrayList()

    override fun getTS(id: Long): TS {
        return tss[(id as Number).toInt() - 1]
    }

    override fun addTS(): TS {
        val ts = CustomTS(MemoryTS(g, nextTSId()))
        tss += ts
        return ts
    }

    override fun nextTSId(): Long = tss.size.toLong() + 1

    override fun clear() {
        tss.clear()
    }
}
