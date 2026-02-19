package it.unibo.tests

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.ingestion.SmartBenchDataLoader
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Paths
import java.util.*
import kotlin.math.round


class TestIngestion {
    private val logger = LoggerFactory.getLogger(TestIngestion::class.java)

    data class IngestionResult(
        val startTimestamp: Long,
        val endTimestamp: Long,
        val graphLoadingTime: Long,
        val tsLoadingTime: Long
    )

    // Test params set through env variables
    private val iterations = System.getenv("INGESTION_ITERATIONS")?.toInt() ?: 1
    private val threads = System.getenv("THREAD")?.toInt() ?: 1
    private val dataset_size = System.getenv("DATASET_SIZE") ?: "small"

    // Folder path for storage consuption
    private val asterixDataFolder = System.getenv("ASTERIXDB_DATA_FOLDER") ?: "asterix_data"
    private val graphDataFolder = System.getenv("GRAPH_DATA_FOLDER") ?: "db_graphm"

    // Output folders setup
    private val resultPath = "results/dt_graph/ingestion_time"
    private val resultFolder = File(resultPath)
    private val testUUID: UUID = UUID.randomUUID()
    private val statisticsFile = File(resultFolder, "ingestion_statistics.csv")

    private fun getFolderSize(folderPath: String): Long {
        val folder = File(folderPath)
        if (!folder.exists()) return 0L
        val sizeBytes = folder.walkTopDown()
            .filter { it.isFile }
            .map { it.length() }
            .sum()
        return round(sizeBytes.toDouble() / (1024 * 1024)).toLong()  // Converti in MB
    }

    private fun loadSmartBench(graph: Graph, dataPath: List<String>): IngestionResult {
        val loader = SmartBenchDataLoader(graph)
        val startTimestamp = System.currentTimeMillis() / 1000
        val ingestionTime = loader.loadData(dataPath, threads)
        val endTimestamp = System.currentTimeMillis() / 1000
        return IngestionResult(startTimestamp, endTimestamp, ingestionTime.first, ingestionTime.second)
    }

    private fun loadDataset(dataPath: List<String>, threads: Int, dataset: String = "smartbench") {
        if (!resultFolder.exists()) resultFolder.mkdirs()

        val dtGraph: Graph
        dtGraph = MemoryGraphACID()
        val tsm = AsterixDBTSM.createDefault(dtGraph)
        dtGraph.tsm = tsm

        // Clear graph and TS data
        dtGraph.clear()
        dtGraph.getTSM().clear()

        val ingestionStats = when (dataset) {
            "smartbench" -> loadSmartBench(dtGraph, dataPath)
            else -> IngestionResult(-1, -1, -1, -1)
        }

        // Flush graph to disk to be queried
        dtGraph.flushToDisk() // Persist graph state to disk

        //Log statistics to file
        val writeHeader = !statisticsFile.exists()
        statisticsFile.appendText(buildString {
            if (writeHeader) append("test_id,model,startTimestamp,endTimestamp,dataset,datasetSize,threads,graphElapsedTime,tsElapsedTime,elapsedTime,numMachines,storage\n")
            append(
                "$testUUID,stgraph,${ingestionStats.startTimestamp},${ingestionStats.endTimestamp},$dataset,$dataset_size,$threads,${ingestionStats.graphLoadingTime},${ingestionStats.tsLoadingTime},${ingestionStats.graphLoadingTime + ingestionStats.tsLoadingTime},${
                    System.getenv(
                        "DEFAULT_NC_POOL"
                    )?.split(',')?.size ?: 1
                },${getFolderSize(asterixDataFolder) + getFolderSize(graphDataFolder)}\n"
            )
        })
    }

    @Test
    fun testSmartBenchIngestion() {
        val projectRoot = Paths.get("").toAbsolutePath().normalize()
        val starting_path="$projectRoot/datasets"
        val data: List<String> = listOf(
            "$starting_path/dataset/smartbench/$dataset_size/group.json",
            "$starting_path/dataset/smartbench/$dataset_size/user.json",
            "$starting_path/dataset/smartbench/$dataset_size/platformType.json",
            "$starting_path/dataset/smartbench/$dataset_size/sensorType.json",
            "$starting_path/dataset/smartbench/$dataset_size/platform.json",
            "$starting_path/dataset/smartbench/$dataset_size/infrastructureType.json",
            "$starting_path/dataset/smartbench/$dataset_size/infrastructure.json",
            "$starting_path/dataset/smartbench/$dataset_size/sensor.json",
            "$starting_path/dataset/smartbench/$dataset_size/virtualSensorType.json",
            "$starting_path/dataset/smartbench/$dataset_size/virtualSensor.json",
            "$starting_path/dataset/smartbench/$dataset_size/semanticObservationType.json",
        )
        repeat(iterations) { i ->
            logger.info("\n=== RUN INGESTION ITERATION #${i + 1} ===")
            loadDataset(data, threads, "smartbench")
        }
    }
}