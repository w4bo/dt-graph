package it.unibo.tests.mimic

import it.unibo.graph.asterixdb.AsterixDBTS
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.utils.*
import kotlin.math.roundToLong

class MimicIVSTGraph(limit: Long) : AbstractMimicIVLoader(limit)  {
    val g = MemoryGraphACID(path = "datasets/dump/mimic/$limit")
    val t = AsterixDBTSM.createDefault(g, "mimic" + if (limit == Long.MAX_VALUE) "" else { "_$limit" })
    var ts: AsterixDBTS? = null
    var i = 0

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

    override fun addTimeseries(row: Map<String, Any?>, person: Long) {
        val n = g.addNode(row["abbreviation"].toString(), isTs = true)
        g.addProperty(n.id, key = "unitname", value = row["unitname"].toString(), PropType.STRING)
        g.addProperty(n.id, key = "category", value = row["category"].toString(), PropType.STRING)
        g.addProperty(n.id, key = "label", value = row["label"].toString(), PropType.STRING)
        g.addProperty(n.id, key = "itemid", value = row["itemid"].toString().toInt(), PropType.INT)
        g.addProperty(n.id, key = "param_type", value = row["param_type"].toString(), PropType.STRING)
        val portRange = LASTFEEDPORT - FIRSTFEEDPORT
        if (i++ > portRange - portRange * 0.2) {
            resetPort()
            i = 0
        }
        ts = g.getTSM().addTS(n.id) as AsterixDBTS
        g.addEdge("hasParameters", fromNode = person, toNode = n.id)
    }

    override fun addMeasurement(row: Map<String, Any?>) {
        ts!!.add(Measurement, timestamp = s2ts(row["charttime"].toString()), value = row["valuenum"].toString().toDouble().roundToLong())
    }

    override fun close() {
        g.flushToDisk()
        g.close()
    }
}