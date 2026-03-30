package it.unibo.tests.mimic.loaders
import it.unibo.graph.asterixdb.dateToTimestamp
import it.unibo.graph.utils.Measurement
import it.unibo.graph.utils.getTime
import it.unibo.stats.Loader
import it.unibo.tests.smartbench.loaders.TSRecord
import java.io.File
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.logging.Logger
import kotlin.math.roundToLong

interface MimicIVLoader: Loader {
    fun addPerson(subjectId: Int): Long
    fun addTimeseries(abbreviation: String, unitname: String?, category: String, label: String, itemid: Int, person: Long): Long
    fun addMeasurement(tsId: Long, row: TSRecord, isLast: Boolean)
}

abstract class AbstractMimicIVLoader(val limit: Long, val threads: Int): MimicIVLoader {
    val logger = Logger.getLogger(this.javaClass.name)
    val url = "jdbc:postgresql://137.204.72.88:5434/mimic-iv"
    val user = "postgres"
    val password = "p0st615*"
    var gsTime: Long = 0
    var tsTime: Long = 0
    var gsCard: Long = 0
    var tsCard: Long = 0
    val item2nodeid: MutableMap<Pair<Long, Long>, Long> = mutableMapOf()
    var users = mutableListOf<Pair<Int, Int>>()

    override fun getGSCardinality(): Long = gsCard
    override fun getTSCardinality(): Long = tsCard
    override fun getGSTime(): Long = gsTime
    override fun getTSTime(): Long = tsTime
    override fun getIndexTime(): Long = 0L

    fun addGs() {
        data class MIMICTS(
            val abbreviation: String,
            val unitname: String?,
            val category: String,
            val label: String,
            val itemid: Int
        )
        DriverManager.getConnection(url, user, password).use { conn ->
            conn.autoCommit = false
            val rows = File("src/main/resources/mimic-iv_subjectids_tsids.csv")
                .useLines { lines ->
                    lines
                        // .drop(1) // uncomment if header exists
                        .map { line ->
                            val (subjectId, itemId, c) = line.trim().split(",")
                            Triple(subjectId.toInt(), itemId.toInt(), c.toInt())
                        }
                        .toList()
                }
            val tsCache = mutableMapOf<Int, MIMICTS>()
            val sql = """SELECT itemid, unitname, category, label, abbreviation FROM d_items""".trimIndent()
            conn.prepareStatement(sql).use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    val ts = MIMICTS(
                        rs.getString("abbreviation"),
                        rs.getString("unitname"),
                        rs.getString("category"),
                        rs.getString("label"),
                        rs.getInt("itemid")
                    )
                    tsCache[ts.itemid] = ts
                }
            }
            val grouped = rows.groupBy({ it.first }) { row ->
                val ts = tsCache[row.second] ?: error("Missing TS for itemId ${row.second}")
                Pair(ts, row.third)
            }
            var acc = 0
            gsTime += getTime {
                for ((subjectId, items) in grouped) {
                    if (acc > limit) break
                    acc += items.sumOf { it.second }
                    users.addAll(items.map{ Pair(subjectId, it.first.itemid) })
                    val person = addPerson(subjectId)
                    gsCard++
                    for ((itemId, _) in items) {
                        val tsId = addTimeseries(itemId.abbreviation, itemId.unitname, itemId.category, itemId.label, itemId.itemid, person)
                        gsCard++
                        item2nodeid[Pair(subjectId.toLong(), itemId.itemid.toLong())] = tsId
                    }
                }
            }
        }
    }

    fun loadTS(pairs: List<Pair<Int, Int>>): Long {
        val events = mutableListOf<Pair<Long, TSRecord>>()
        // val origin  = mutableListOf<Triple<Any, Any, Any>>()
        val subjectids = pairs.joinToString { (subjectid, _) -> "'$subjectid'" }
        val itemids = pairs.map { (_, itemId) -> itemId }.toSet().joinToString { itemId -> "'$itemId'" }
        DriverManager.getConnection(url, user, password).use { conn ->
            conn.autoCommit = false
            val sql = """
                SELECT subject_id, itemid, charttime, avg(cast(valuenum as float)) as valuenum
                FROM chartevents c 
                WHERE c.subject_id in ($subjectids) and c.itemid in ($itemids) and valuenum is not null
                GROUP BY subject_id, itemid, charttime
                ORDER BY subject_id, itemid, charttime
                ${if (limit != Long.MAX_VALUE) "LIMIT $limit" else ""}
            """.trimIndent()
            logger.info(sql)
            conn.prepareStatement(sql).use { stmt ->
                stmt.fetchSize = 100_000
                val rs: ResultSet = stmt.executeQuery()
                while (rs.next()) {
                    // origin.add(Triple(rs.getString("subject_id"), rs.getString("itemid"), rs.getString("charttime")))
                    val subjectId = rs.getLong("subject_id")
                    val itemId = rs.getLong("itemid")
                    if (pairs.contains(Pair(subjectId.toInt(), itemId.toInt()))) {
                        val tsId = item2nodeid[Pair(subjectId, itemId)]
                        val chartTime = rs.getString("charttime")
                        val valueNum = rs.getString("valuenum")
                        events.add(Pair(tsId!!, TSRecord(Measurement, timestamp = dateToTimestamp(chartTime), location = "", value = valueNum.toDouble().roundToLong())))
                    }
                }
            }
        }
        tsCard += events.size
        val time = getTime {
            events.forEachIndexed { index, pair ->
                val isLast = index + 1 == events.size || events[index].first != events[index + 1].first
                addMeasurement(pair.first, pair.second, isLast = isLast)
            }
        }
        return time
    }

    fun addTs() {
        val chunkSize = users.size / threads
        val results = LongArray(threads)
        val workers = mutableListOf<Thread>()
        for (i in 0 until threads) {
            val start = i * chunkSize
            val end = if (i == threads - 1) users.size else (i + 1) * chunkSize
            val subList = users.subList(start, end)
            val t = Thread {
                val time = loadTS(subList)
                results[i] = time
            }
            workers += t
            t.start()
        }
        workers.forEach { it.join() }
        tsTime = results.maxOrNull() ?: -1L
    }

    override fun loadData() {
        addGs()
        addTs()
        close()
    }
}

