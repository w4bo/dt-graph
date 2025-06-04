import it.unibo.evaluation.dtgraph.SmartBenchDataLoader
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Labels
import it.unibo.graph.query.*
import it.unibo.graph.structure.CustomGraph
import kotlin.test.Test

class TestSmartBench {

    private fun setup(): Graph {
        val data = listOf(
            "group.json",
            "user.json",
            "platformType.json",
            "sensorType.json",
            "platform.json",
            "infrastructureType.json",
            "infrastructure.json",
            "sensor.json",
            "virtualSensorType.json",
            "virtualSensor.json",
            "semanticObservationType.json",
            "semanticObservation.json",
            "observation.json"
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
        val graph = setup()
        print("I should be done")
        val a = search(graph, listOf(Step(Labels.Sensor)))
        val sensor = a[0]
        print("hello")
    }
}