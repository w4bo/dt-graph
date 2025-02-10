package it.unibo.graph

interface TSManager {
    fun addTS(): TS
    fun nextTSId(): Int
    fun getTS(id: Int): TS
    fun clear()
}

class MemoryTSManager : TSManager {
    private val tss: MutableList<TS> = ArrayList()

    override fun getTS(id: Int): TS {
        return tss[id]
    }

    override fun addTS(): TS {
        val ts = CustomTS(MemoryTS(nextTSId()))
        tss += ts
        return ts
    }

    override fun nextTSId(): Int = tss.size

    override fun clear() {
        tss.clear()
    }
}