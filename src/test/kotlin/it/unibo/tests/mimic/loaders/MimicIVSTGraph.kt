package it.unibo.tests.mimic.loaders

import it.unibo.graph.asterixdb.AsterixDBTS
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.utils.*
import it.unibo.tests.smartbench.loaders.TSRecord
import kotlin.collections.set

class MimicIVSTGraph(
    size: String,
    threads: Int = 1,
    host: String = "localhost",
    controllerIPs: List<String> = listOf("localhost")
) : AbstractMimicIVLoader(if (size == "full") Long.MAX_VALUE else size.toLong(), threads = threads) {
    val s = if (size == "full" || size.toLong() == Long.MAX_VALUE) "full" else size
    val g = MemoryGraphACID(path = "datasets/dump/mimic/$s")
    val t = AsterixDBTSM.createDefault(g, "mimic_$s", host = host, controllerIps = controllerIPs, maxConnections = if (threads == 1) 1 else { threads * 10 })
    var i = 0
    val tsMap = mutableMapOf<Long, AsterixDBTS>()
    val set = mutableSetOf<TSRecord>()

    init {
        g.tsm = t
        g.clear()
        t.clear()
    }

    override fun addPerson(subjectId: Int): Long {
        val person = g.addNode(Person)
        g.addProperty(person.id, key = "subject_id", value = subjectId, PropType.INT)
        return person.id
    }

    override fun addTimeseries(abbreviation: String, unitname: String?, category: String, label: String, itemid: Int, person: Long): Long {
        val n = g.addNode(abbreviation, isTs = true)
        if (unitname != null) g.addProperty(n.id, key = "unitname", value = unitname, PropType.STRING)
        g.addProperty(n.id, key = "category", value = category, PropType.STRING)
        g.addProperty(n.id, key = "label", value = label, PropType.STRING)
        g.addProperty(n.id, key = "itemid", value = itemid, PropType.INT)
        val ts = (g.getTSM() as AsterixDBTSM).addTS(n.id) as AsterixDBTS
        tsMap[n.id] = ts
        g.addEdge("hasParameters", fromNode = person, toNode = n.id)
        return n.id
    }

    override fun addMeasurement(tsId: Long, row: TSRecord, isLast: Boolean) {
        val ts = tsMap[tsId]!!
        ts.add(
            row.type,
            timestamp = row.timestamp,
            value = row.value as Long,
            flush = false
        )
        if (isLast) {
            ts.connection.flush()
        }
    }

    override fun close() {
        g.flushToDisk()
        g.close()
    }
}