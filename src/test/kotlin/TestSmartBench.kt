import it.unibo.evaluation.dtgraph.SmartBenchDataLoader
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.structure.CustomGraph
import java.io.File
import kotlin.test.Test

class TestSmartBench {

    private fun getFolderSize(folder: File): Long {
        var size: Long = 0
        if (folder.exists() && folder.isDirectory) {
            folder.listFiles()?.forEach { file ->
                size += if (file.isFile) {
                    file.length()
                } else {
                    getFolderSize(file)
                }
            }
        }
        return size
    }

    private fun setup(dataset: String, size: String): Graph {
        val data : List<String> = listOf(
            "dataset/$dataset/$size/group.json",
            "dataset/$dataset/$size/user.json",
            "dataset/$dataset/$size/platformType.json",
            "dataset/$dataset/$size/sensorType.json",
            "dataset/$dataset/$size/platform.json",
            "dataset/$dataset/$size/infrastructureType.json",
            "dataset/$dataset/$size/infrastructure.json",
            "dataset/$dataset/$size/sensor.json",
            "dataset/$dataset/$size/virtualSensorType.json",
            "dataset/$dataset/$size/virtualSensor.json",
            "dataset/$dataset/$size/semanticObservationType.json",
            //"dataset/$dataset/$size/semanticObservation.json",
            "dataset/$dataset/$size/observation.json"
        )
        val g = CustomGraph(MemoryGraph())
        g.tsm = AsterixDBTSM.createDefault(g)
        g.clear()
        g.getTSM().clear()

        val loader = SmartBenchDataLoader(g)
        loader.loadData(data)
        return g
    }

    @Test
    fun testSum() {
        val graph = setup("smartbench","small")
        println("Should be done")
        println("Loaded ${graph.getNodes().size} verticles")
        println("Loaded ${graph.getEdges().size} edges")
        println("Loaded ${graph.getProps().size} props")
    }
}