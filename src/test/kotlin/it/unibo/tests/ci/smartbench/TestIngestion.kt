package it.unibo.tests.ci.smartbench

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.utils.resetPort
import it.unibo.stats.checkFolder
import it.unibo.stats.loadDataset
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import java.util.UUID

class TestIngestion {
    private val logger = LoggerFactory.getLogger(TestIngestion::class.java)

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
                checkFolder(graphDataFolder)
                checkFolder(asterixDataFolder)

                val graph = MemoryGraphACID(path = "datasets/dump/$dataset/$size")
                val tsm = AsterixDBTSM.Companion.createDefault(graph, dataverse = "${dataset}_$size")
                graph.tsm = tsm
                graph.clear()
                tsm.clear()

                loadDataset(loader = SmartBenchDataLoader(graph, threads, data), "stgraph", threads, numMachines, dataset, size)

                graph.close()
            }
        }
    }
}