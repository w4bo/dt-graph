package it.unibo.tests.ci.smartbench

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.utils.resetPort
import it.unibo.stats.loadDataset
import it.unibo.tests.ci.smartbench.loaders.SmartBenchDataLoader
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Paths

class TestSmartBenchIngestion {
    private val logger = LoggerFactory.getLogger(TestSmartBenchIngestion::class.java)

    @Test
    fun testSmartBenchIngestion() {
        val sizes = listOf("small", "medium", "large")
        val projectRoot = Paths.get("").toAbsolutePath().normalize()
        val path = "$projectRoot/datasets"
        val iterations = 1
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
            repeat(iterations) { i ->
                logger.info("\n--- Ingesting $dataset/$size. Iteration: #${i + 1}")
                resetPort()
                val graphPath = System.getenv("GRAPH_DATA_FOLDER") ?: "datasets/dump/$dataset/$size/"
                val path = Paths.get(graphPath)
                if (!Files.exists(path)) {
                    Files.createDirectories(path)
                }
                val graph = MemoryGraphACID(path = graphPath)
                val tsm = AsterixDBTSM.createDefault(graph, dataverse = "${dataset}_$size")
                graph.tsm = tsm
                graph.clear()
                tsm.clear()
                loadDataset(loader = SmartBenchDataLoader(graph, threads = 1, data), "stgraph", threads = 1, numMachines = 1, dataset, size)
                graph.close()
            }
        }
    }
}