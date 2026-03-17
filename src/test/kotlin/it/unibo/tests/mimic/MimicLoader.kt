package it.unibo.tests.mimic
import it.unibo.stats.Loader
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.use

fun s2ts(s: String): Long = LocalDateTime.parse(s.replace(" ", "T")).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()

interface MimicIVLoader: Loader {
    fun addPerson(subjectId: Int): Long
    fun addTimeseries(row: Map<String, Any?>, person: Long)
    fun addMeasurement(row: Map<String, Any?>)
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

    override fun loadData() {
        val sql = """
        select *
        from (SELECT * FROM chartevents where valuenum is not null ${if (limit == Long.MAX_VALUE) "" else { "LIMIT $limit" }}) c 
            JOIN d_items i ON (c.itemid = i.itemid)
            JOIN icustays s ON (c.stay_id = s.stay_id)
        order by c.subject_id, c.itemid, c.charttime
    """.trimIndent()

        var lastSubjectId: Int? = null
        var lastItemId: Int? = null
        var person: Long? = null
        var i = 0
        println("=== Loader: ${javaClass}: ")

        DriverManager.getConnection(url, user, password).use { conn ->
            conn.autoCommit = false
            conn.prepareStatement(sql).use { stmt ->
                stmt.fetchSize = 1000
                val rs: ResultSet = stmt.executeQuery()
                val meta = rs.metaData
                val columnCount = meta.columnCount
                while (rs.next()) {
                    val row = mutableMapOf<String, Any?>()
                    for (i in 1..columnCount) {
                        val columnName = meta.getColumnLabel(i)   // works with aliases
                        val value = rs.getObject(i)
                        row[columnName] = value
                    }
                    if (i++ % 100 == 0) print("$i... ")
                    val subjectId: Int = row["subject_id"].toString().toInt()
                    val itemId: Int = row["itemid"].toString().toInt()
                    if (subjectId != lastSubjectId) {
                        lastSubjectId = subjectId
                        val startTime = System.currentTimeMillis()
                        person = addPerson(subjectId)
                        gsTime += System.currentTimeMillis() - startTime
                    }
                    if (itemId != lastItemId) {
                        lastItemId = itemId
                        val startTime = System.currentTimeMillis()
                        addTimeseries(row, person!!)
                        gsTime += System.currentTimeMillis() - startTime
                    }
                    val startTime = System.currentTimeMillis()
                    addMeasurement(row)
                    tsTime += System.currentTimeMillis() - startTime
                }
            }
        }
        close()
    }
}