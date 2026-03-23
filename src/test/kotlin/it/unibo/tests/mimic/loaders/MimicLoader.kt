package it.unibo.tests.mimic.loaders
import it.unibo.stats.Loader
import java.io.File
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.system.measureTimeMillis

fun s2ts(s: String): Long = LocalDateTime.parse(s.replace(" ", "T")).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()

interface MimicIVLoader: Loader {
    fun addPerson(subjectId: Int): Long
    fun addTimeseries(row: ResultSet, person: Long)
    fun addMeasurement(row: ResultSet)
    fun close()
}

abstract class AbstractMimicIVLoader(val limit: Long): MimicIVLoader {
    val url = "jdbc:postgresql://137.204.72.88:5434/mimic-iv"
    val user = "postgres"
    val password = "p0st615*"

    override fun getGSTime(): Long = gsTime

    override fun getTSTime(): Long = tsTime

    var gsTime: Long = 0
    var tsTime: Long = 0

    override fun getIndexTime(): Long = 0L

    override fun loadData() {
        var lastItemId: Int? = null
        var person: Long? = null
        var i = 0
        println("=== Loader: ${javaClass}: ")
        var count = 0L
        DriverManager.getConnection(url, user, password).use { conn ->
            conn.autoCommit = false
            File("src/main/resources/mimic-iv_subjectids.csv").useLines { lines ->
                lines.drop(1).forEach { line ->  // skip first line
                    val subjectId = line.trim()
                    if (subjectId.isEmpty()) return@forEach
                    val sql = """
                        SELECT c.subject_id, c.itemid, unitname, category, label, param_type, charttime, abbreviation, valuenum
                        FROM chartevents c JOIN d_items i ON (c.itemid = i.itemid) JOIN icustays s ON (c.stay_id = s.stay_id)
                        WHERE c.subject_id = '$subjectId' and valuenum is not null
                        ORDER BY c.subject_id, c.itemid, c.charttime
                        ${if (limit != Long.MAX_VALUE) "LIMIT $limit" else ""}
                        """.trimIndent()
                    var first = true
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.fetchSize = 100_000
                        val rs: ResultSet = stmt.executeQuery()
                        var prevTime = System.currentTimeMillis()
                        var addedTS = 0
                        var addedGS = 0
                        var addedMeas = 0
                        while (rs.next()) {
                            if (++i % 10000 == 0) {
                                println("$i... Elapsed time: ${(System.currentTimeMillis() - prevTime)}, GS: $addedGS, TS: $addedTS, Mea: $addedMeas")
                                prevTime = System.currentTimeMillis()
                                addedGS = 0
                                addedMeas = 0
                                addedTS = 0
                            }
                            val subjectId: Int = rs.getString("subject_id").toInt()
                            val itemId: Int = rs.getString("itemid").toInt()
                            if (first) {
                                first = false
                                gsTime += measureTimeMillis {  person = addPerson(subjectId) }
                                addedGS++
                            }
                            if (itemId != lastItemId) {
                                lastItemId = itemId
                                gsTime += measureTimeMillis { addTimeseries(rs, person!!) }
                                addedGS++
                                addedTS++
                            }
                            tsTime += measureTimeMillis { addMeasurement(rs) }
                            addedMeas++
                            if (++count > limit) {
                                close()
                                return
                            }
                        }
                    }
                }
            }
        }
        close()
    }
}