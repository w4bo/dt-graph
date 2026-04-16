package it.unibo.tests.mimic.loaders
import it.unibo.graph.asterixdb.dateToTimestamp
import it.unibo.graph.utils.Measurement
import it.unibo.graph.utils.getTime
import it.unibo.stats.Loader
import it.unibo.tests.smartbench.loaders.TSRecord
import java.io.File
import java.sql.DriverManager
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
                    users.addAll(items.map { Pair(subjectId, it.first.itemid) })
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
        val chunkSize = 100_000
        pairs.chunked(chunkSize).forEach { chunk ->
            val chunk = chunk.toSet()
            val subjectIds = chunk.map { (subjectId, _) -> subjectId }.toSet().joinToString { "'$it'" }
            val itemIds = chunk.map { (_, itemId) -> itemId }.toSet().joinToString { "'$it'" }
            DriverManager.getConnection(url, user, password).use { conn ->
                conn.autoCommit = false
                val sql = """
                    SELECT subject_id, itemid, charttime, valuenum
                    FROM chartevents_unique c 
                    WHERE c.subject_id IN ($subjectIds) AND c.itemid IN ($itemIds)
                    ORDER BY subject_id, itemid, charttime
                """.trimIndent()
                logger.info(sql)
                conn.prepareStatement(sql).use { stmt ->
                    stmt.fetchSize = 10_000   // smaller is safer
                    val rs = stmt.executeQuery()
                    var prevItemId = -1L
                    var prevSubjectId = -1L
                    var skip = false
                    while (rs.next()) {
                        val subjectId = rs.getLong("subject_id")
                        val itemId = rs.getLong("itemid")
                        if (subjectId != prevSubjectId || itemId != prevItemId) {
                            skip = !chunk.contains(subjectId.toInt() to itemId.toInt())
                            prevItemId = itemId
                            prevSubjectId = subjectId
                        }
                        if (skip) continue
                        val tsId = item2nodeid[subjectId to itemId]
                        val chartTime = rs.getString("charttime")
                        val valueNum = rs.getString("valuenum")
                        events.add(tsId!! to TSRecord(Measurement, timestamp = dateToTimestamp(chartTime), location = "", value = valueNum.toDouble().roundToLong()))
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

