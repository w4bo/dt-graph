package it.unibo.stats

import java.io.File
import java.util.UUID
import kotlin.math.round

interface Loader {
    fun getGSTime(): Long
    fun getTSTime(): Long
    fun loadData()
}

const val resultPath = "results/dt_graph/ingestion_time"
val resultFolder = File(resultPath)
val statisticsFile = File(resultFolder, "ingestion_statistics.csv")

fun checkFolder(folderPath: String) {
    val folder = File(folderPath)
    if (!folder.exists()) throw IllegalStateException("Folder $folderPath does not exist")
}

private fun getFolderSize(folderPath: String): Long {
    val folder = File(folderPath)
    val sizeBytes = folder.walkTopDown().filter { it.isFile }.map { it.length() }.sum()
    return round(sizeBytes.toDouble() / (1024 * 1024)).toLong() // Converti in MB
}

fun loadDataset(loader: Loader, model: String, threads: Int, numMachines: Int, dataset: String, size: String) {
    if (!resultFolder.exists()) resultFolder.mkdirs()
    val startTimestamp = System.currentTimeMillis() / 1000
    loader.loadData()
    val endTimestamp = System.currentTimeMillis() / 1000
    val gsStorage = -1
    val tsStorage = -1
    val row = linkedMapOf(
        "test_id" to UUID.randomUUID().toString(),
        "model" to model,
        "startTimestamp" to startTimestamp,
        "endTimestamp" to endTimestamp,
        "dataset" to dataset,
        "datasetSize" to size,
        "threads" to threads,
        "graphElapsedTime" to loader.getGSTime(),
        "tsElapsedTime" to loader.getTSTime(),
        "elapsedTime" to loader.getGSTime() + loader.getTSTime(),
        "numMachines" to numMachines,
        "storage" to gsStorage + tsStorage,
        "tsStorage" to tsStorage,
        "gsStorage" to gsStorage
    )

    val writeHeader = !statisticsFile.exists()
    statisticsFile.appendText(buildString {
        if (writeHeader) append(row.keys.joinToString(",") + "\n")
        append(row.values.joinToString(",") + "\n")
    })
}