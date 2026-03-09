package it.unibo.tests.ci

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.utils.resetPort
import it.unibo.ingestion.SmartBenchDataLoader
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Paths
import java.util.*
import kotlin.math.round

// Output folders setup
const val resultPath = "results/dt_graph/ingestion_time"
val resultFolder = File(resultPath)
val statisticsFile = File(resultFolder, "ingestion_statistics.csv")

class TestIngestion {
    private val logger = LoggerFactory.getLogger(TestIngestion::class.java)

    private fun checkFolder(folderPath: String) {
        val folder = File(folderPath)
        if (!folder.exists()) throw IllegalStateException("Folder $folderPath does not exist")
    }

    private fun getFolderSize(folderPath: String): Long {
        val folder = File(folderPath)
        val sizeBytes = folder.walkTopDown().filter { it.isFile }.map { it.length() }.sum()
        return round(sizeBytes.toDouble() / (1024 * 1024)).toLong() // Converti in MB
    }

    private fun loadDataset(uuid: String, dataPath: List<String>, threads: Int, numMachines: Int, dataset: String, size: String, graphDataFolder: String, asterixDataFolder: String) {
        if (!resultFolder.exists()) resultFolder.mkdirs()
        checkFolder(graphDataFolder)
        checkFolder(asterixDataFolder)

        val graph = MemoryGraphACID(path ="datasets/dump/$dataset/$size")
        val tsm = AsterixDBTSM.createDefault(graph, dataverse = "${dataset}_$size")
        graph.tsm = tsm
        graph.clear()
        tsm.clear()

        val startTimestamp = System.currentTimeMillis() / 1000
        val graphLoadingTime: Long
        val tsLoadingTime: Long
        when (dataset) {
            "smartbench" -> {
                val ingestionTime = SmartBenchDataLoader(graph).loadData(dataPath, threads)
                graphLoadingTime = ingestionTime.first
                tsLoadingTime = ingestionTime.second
            }
            else -> throw IllegalArgumentException("$dataset not supported yet")
        }
        graph.flushToDisk() // Persist graph state to disk
        val endTimestamp = System.currentTimeMillis() / 1000

        val gsStorage = getFolderSize(graphDataFolder)
        val tsStorage = getFolderSize(asterixDataFolder)
        val row = linkedMapOf(
            "test_id" to uuid,
            "model" to "stgraph",
            "startTimestamp" to startTimestamp,
            "endTimestamp" to endTimestamp,
            "dataset" to dataset,
            "datasetSize" to size,
            "threads" to threads,
            "graphElapsedTime" to graphLoadingTime,
            "tsElapsedTime" to tsLoadingTime,
            "elapsedTime" to tsLoadingTime + graphLoadingTime,
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

        graph.close()
    }

    @Test
    fun testSmartBenchIngestion() {
        val sizes = listOf("small") // listOf("small", "medium", "large") // System.getenv("DATASET_SIZE")

        val asterixDataFolder = System.getenv("ASTERIXDB_DATA_FOLDER") ?: "datasets/dump/asterixdb"
        val projectRoot = Paths.get("").toAbsolutePath().normalize()
        val path = "$projectRoot/datasets"
        val iterations = System.getenv("INGESTION_ITERATIONS")?.toInt() ?: 1
        val threads = System.getenv("THREAD")?.toInt() ?: 1
        val numMachines = System.getenv("DEFAULT_NC_POOL")?.split(',')?.size ?: 1
        val dataset = "smartbench"

        sizes.forEach { size ->
            val data: List<String> = listOf(
                "$path/original/$dataset/$size/group.json",
                "$path/original/$dataset/$size/user.json",
                "$path/original/$dataset/$size/platformType.json",
                "$path/original/$dataset/$size/sensorType.json",
                "$path/original/$dataset/$size/platform.json",
                "$path/original/$dataset/$size/infrastructureType.json",
                "$path/original/$dataset/$size/infrastructure.json",
                "$path/original/$dataset/$size/sensor.json",
                "$path/original/$dataset/$size/virtualSensorType.json",
                "$path/original/$dataset/$size/virtualSensor.json",
                "$path/original/$dataset/$size/semanticObservationType.json",
            )
            val graphDataFolder = System.getenv("GRAPH_DATA_FOLDER") ?: "datasets/dump/$dataset/$size/"
            repeat(iterations) { i ->
                logger.info("\n--- Ingesting $dataset/$size. Iteration: #${i + 1}")
                resetPort()
                loadDataset(UUID.randomUUID().toString(), data, threads, numMachines, dataset, size, graphDataFolder, asterixDataFolder)
            }
        }
    }
}