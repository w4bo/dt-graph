package it.unibo.tests.mimic.loaders

import it.unibo.graph.asterixdb.AsterixDBTS
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.utils.*
import java.sql.ResultSet
import kotlin.math.roundToLong

class MimicIVSTGraph(limit: Long, controllerIps: List<String> = listOf("localhost")) : AbstractMimicIVLoader(limit)  {
    val s = if (limit == Long.MAX_VALUE) "full" else "$limit"
    val g = MemoryGraphACID(path = "datasets/dump/mimic/$s")
    val t = AsterixDBTSM.createDefault(g, "mimic_$s", controllerIps = controllerIps)
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

    override fun addTimeseries(row: ResultSet, person: Long) {
        val n = g.addNode(row.getString("abbreviation"), isTs = true)
        val unitname: String? = row.getString("unitname")
        if (unitname != null) g.addProperty(n.id, key = "unitname", value = unitname, PropType.STRING)
        g.addProperty(n.id, key = "category", value = row.getString("category"), PropType.STRING)
        g.addProperty(n.id, key = "label", value = row.getString("label"), PropType.STRING)
        g.addProperty(n.id, key = "itemid", value = row.getString("itemid").toInt(), PropType.INT)
        // g.addProperty(n.id, key = "param_type", value = row.getString("param_type"), PropType.STRING)
        val portRange = LASTFEEDPORT - FIRSTFEEDPORT
        if (i++ > portRange - portRange * 0.2) {
            resetPort()
            i = 0
        }
        ts = g.getTSM().addTS(n.id) as AsterixDBTS
        g.addEdge("hasParameters", fromNode = person, toNode = n.id)
    }

    override fun addMeasurement(row: ResultSet) {
        ts!!.add(
            Measurement,
            timestamp = s2ts(row.getString("charttime")),
            value = row.getString("valuenum").toDouble().roundToLong()
        )
    }

    override fun close() {
        g.flushToDisk()
        g.close()
    }
}