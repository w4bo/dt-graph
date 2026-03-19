package it.unibo.stats

import java.io.File
import java.util.UUID

private const val resultPath = "results/dt_graph/query_time"
private val resultFolder = File(resultPath)
private val statisticsFile = File(resultFolder, "query_statistics.csv")

class QueryResultData(val timeMs: Long, val card: Int)
interface Querying {
    val queryId: String
    fun runQuery(): QueryResultData
}

fun runQuery(querying: Querying, model: String, threads: Int, numMachines: Int, dataset: String, size: String) {
    if (!resultFolder.exists()) resultFolder.mkdirs()
    val startTimestamp = System.currentTimeMillis() / 1000
    val data = querying.runQuery()
    val endTimestamp = System.currentTimeMillis() / 1000
    val row = linkedMapOf(
        "test_id" to UUID.randomUUID().toString(),
        "queryid" to querying.queryId, // Q1, Q2, ...
        "model" to model, // STGraph, Neo4J, ...
        "dataset" to dataset, // SmartBench, Mimic-IV
        "datasetSize" to size, // small, medium, ...
        "threads" to threads,
        "elapsedTime" to endTimestamp - startTimestamp,
        "numMachines" to numMachines,
        "cardinality" to data.card
    )

    val writeHeader = !statisticsFile.exists()
    statisticsFile.appendText(buildString {
        if (writeHeader) append(row.keys.joinToString(",") + "\n")
        append(row.values.joinToString(",") + "\n")
    })
}