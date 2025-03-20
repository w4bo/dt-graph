import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.interfaces.TS
import it.unibo.graph.query.*
import it.unibo.graph.rocksdb.RocksDBGraph
import it.unibo.graph.structure.CustomGraph
import it.unibo.graph.utils.*
import org.apache.tinkerpop.gremlin.process.traversal.Pop
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestFactory
import kotlin.test.Test
import kotlin.test.assertTrue
import it.unibo.graph.interfaces.Labels.*

class TestWorkload{

    val staticDevicePattern = listOf(
        Step(AgriFarm, alias="farm"),
        Step(HasParcel),
        Step(AgriParcel, alias="parcel"),
        Step(HasDevice),
        Step(Device, alias="device"),
    )

    val dynamicDevicePattern = listOf(
        Step(AgriFarm, alias="farm"),
        Step(HasParcel),
        Step(AgriParcel, alias="parcel"),
        Step(HasDrone),
        Step(Drone, alias="device"),
    )
    fun setup(dynamicDevices: Boolean = false): CustomGraph {
        return setup(MemoryGraph(), dynamicDevices)
    }

    fun setup(g: Graph, dynamicDevices: Boolean): CustomGraph {

        // Define a relative timestamp for debug purposes
        val maxTimestamp = 5L

        val g = CustomGraph(g)
        g.tsm = AsterixDBTSM.createDefault(g)
        g.clear()
        g.getTSM().clear()

        // Errano farm
        val errano = g.addNode(AgriFarm)
        g.addProperty(errano.id, "name", "Errano Kiwi Farm",PropType.STRING)
        g.addProperty(errano.id, "location", """{"coordinates":[[[11.798105,44.234354],[11.801217,44.237683],[11.805286,44.235809],[11.803987,44.234851],[11.804789,44.233683],[11.80268,44.231419],[11.798105,44.234354]]],"type":"Polygon"}""", PropType.GEOMETRY)

        // Errano parcels
        val erranoT1 = g.addNode(AgriParcel)
        g.addProperty(erranoT1.id, "name", "Errano T1" ,PropType.STRING)
        g.addProperty(erranoT1.id, "location", """{"coordinates":[[[11.79915,44.235384],[11.799412,44.23567],[11.801042,44.234555],[11.800681,44.234343],[11.79915,44.235384]]],"type":"Polygon"}""" ,PropType.GEOMETRY)
        val erranoT2 = g.addNode(AgriParcel)
        g.addProperty(erranoT2.id, "location", """{"coordinates":[[[11.799569,44.235609],[11.79977,44.235759],[11.801395,44.234782],[11.80108,44.234572],[11.799569,44.235609]]],"type":"Polygon"}""" ,PropType.GEOMETRY)
        g.addProperty(erranoT2.id, "name", "Errano T2" ,PropType.STRING)

        g.addEdge(HasParcel, errano.id, erranoT1.id)
        g.addEdge(HasParcel, errano.id, erranoT2.id)

        // Static devices
        val t1Moisture = g.addNode(Device)
        g.addProperty(t1Moisture.id, "location", """{"coordinates":[11.799328,44.235394],"type":"Point"}""" ,PropType.GEOMETRY)
        g.addProperty(t1Moisture.id, "name", "Errano T1 MoistureDevice" ,PropType.STRING)
        val t2Moisture = g.addNode(Device)
        g.addProperty(t2Moisture.id, "location", """{"coordinates":[11.800711,44.234904],"type":"Point"}""" ,PropType.GEOMETRY)
        g.addProperty(t2Moisture.id, "name", "Errano T2 MoistureDevice" ,PropType.STRING)

        if(dynamicDevices){
            var measurementTimestamp = 0L

            // Moving device
            val erranoDrone = g.addNode(Drone)
            g.addProperty(erranoDrone.id, "name", "Errano Drone" ,PropType.STRING)
            g.addProperty(erranoDrone.id, "location", """{"coordinates":[11.799328,44.235394],"type":"Point"}""", PropType.GEOMETRY, from = 0, to = 2)

            val droneTs = g.getTSM().addTS()
            val droneNDVI = g.addNode(NDVI, value = droneTs.getTSId())

            g.addEdge(HasDrone, erranoT1.id, erranoDrone.id, from = 0, to = 2)
            g.addEdge(HasNDVI, erranoDrone.id, droneNDVI.id)

            droneTs.add(Measurement, timestamp = measurementTimestamp++, value = measurementTimestamp, location = """{"coordinates":[11.799328,44.235394],"type":"Point"}""")
            droneTs.add(Measurement, timestamp = measurementTimestamp++, value = measurementTimestamp, location = """{"coordinates":[11.799328,44.235394],"type":"Point"}""")

            //Moving device from T1 to T2
            g.addEdge(HasDrone, erranoT2.id, erranoDrone.id, from = 2, to = 5)
            g.addProperty(erranoDrone.id, "location", """{"coordinates":[11.800711,44.234904],"type":"Point"}""", PropType.GEOMETRY, from = 2, to = 5)

            droneTs.add(Measurement, timestamp = measurementTimestamp++, value = measurementTimestamp, location = """{"coordinates":[11.800711,44.234904],"type":"Point"}""")
            droneTs.add(Measurement, timestamp = measurementTimestamp++, value = measurementTimestamp, location = """{"coordinates":[11.800711,44.234904],"type":"Point"}""")

        }

        // Weather station, not linked to anything
        val weatherStation = g.addNode(Device)
        g.addProperty(weatherStation.id, "location", """{"coordinates":[11.80164,44.234831],"type":"Point"}""" ,PropType.GEOMETRY)

        // g.addEdge(hasDevice, errano.id, weatherStation.id)
        g.addEdge(HasDevice, erranoT1.id, t1Moisture.id)
        g.addEdge(HasDevice, erranoT2.id, t2Moisture.id)

        // Time series
        val t1TS = g.getTSM().addTS()
        val t2TS = g.getTSM().addTS()
        val weatherTS = g.getTSM().addTS()

        for (timestamp in 0L..1){
            println("Added measruement")
            t1TS.add(Measurement, timestamp = timestamp, value = timestamp, location = """{"coordinates":[11.799328,44.235394],"type":"Point"}""")
            //t2TS.add(Measurement, timestamp = timestamp, value = timestamp, location = """{"coordinates":[11.800711,44.234904],"type":"Point"}""")
            //weatherTS.add(Measurement, timestamp = timestamp, value = timestamp, location = """{"coordinates":[11.80164,44.234831],"type":"Point"}""")
        }

        val weatherTemperature = g.addNode(Temperature, value = weatherTS.getTSId())
        val t1Humidity = g.addNode(Humidity, value = t1TS.getTSId())
        val t2Humidity = g.addNode(Humidity, value = t2TS.getTSId())


        g.addEdge(HasHumidity, t1Moisture.id, t1Humidity.id)
        g.addEdge(HasHumidity, t2Moisture.id, t2Humidity.id)
        g.addEdge(HasTemperature, weatherStation.id, weatherTemperature.id)

        return g
    }

    @Test
    fun testSum() {
        assertEquals(42, 40 + 2)
    }

    @Test
    fun testParcelInFarm() {
        val g = setup()
        // Parcels in farm
        kotlin.test.assertEquals(2, search(g, staticDevicePattern, listOf(Compare("farm", "parcel", "location", Operators.ST_CONTAINS)), timeaware = false).size)
        // Devices in Farm
        kotlin.test.assertEquals(2, search(g, staticDevicePattern, listOf(Compare("parcel", "device", "location", Operators.ST_CONTAINS)), timeaware = false).size)
    }

    @Test
    fun devicesInFarms() {
        val g = setup()
        val pattern = listOf(
                listOf(Step(AgriFarm, alias="farm")),
                listOf(Step(Device, alias="device"))
            )
        // Devices in farm throuh spatial join
        kotlin.test.assertEquals(3, query(g, pattern, listOf(Compare("farm","device", "location", Operators.ST_CONTAINS)), timeaware = false).size)
    }

    // E* -> A*
    @Test
    fun environmentCoverage() {
        val g = setup()
        val tau = Humidity
        val sarchLocation = """{"coordinates":[[[11.798105,44.234354],[11.801217,44.237683],[11.805286,44.235809],[11.803987,44.234851],[11.804789,44.233683],[11.80268,44.231419],[11.798105,44.234354]]],"type":"Polygon"}"""


        // v.1 Following graph edges
        val pattern = staticDevicePattern + listOf(
            Step(HasHumidity),
            Step(tau),
            Step(HasTS)
        )

        // v.2 Following spatial contains
        val targetLocation = g.addNode(TargetLocation)
        g.addProperty(targetLocation.id, "location", sarchLocation, PropType.GEOMETRY)

        val spatialPattern = listOf(
            listOf(Step(TargetLocation, alias="targetLocation")),
            listOf(Step(Device, alias="device"))
        )

        // v.1
        kotlin.test.assertEquals(2, search(g, pattern, listOf(Compare("parcel", "device", "location", Operators.ST_CONTAINS)), timeaware = false).size)

        // v.2
        kotlin.test.assertEquals(3, query(g, spatialPattern, listOf(Compare("targetLocation","device", "location", Operators.ST_CONTAINS)), timeaware = false).size)
    }

    // E* ->  A* -> M
    @Test
    fun environmentAggregate(){
        val g = setup(dynamicDevices=true)
        val tau = NDVI

        // v.1 Following graph edges
        val pattern = dynamicDevicePattern + listOf(
            null,
            Step(tau),
            Step(HasTS)
        )
        val result = search(g, pattern )
        kotlin.test.assertEquals(2, result.size)
    }

    // A -> M -> E
//    @Test
//    fun AgentCoverage(){
//        val g = setup(dynamicDevices=true)
//        val devices = listOf(Pair(1,"Errano T1 MoistureDevice"), Pair(1,"Errano T1 MoistureDevice"), Pair(2,"Errano Drone"))
//        devices.forEach {
//            val pattern = listOf(
//                listOf(Step(AgriParcel, alias = "env")),
//                listOf(Step(null, listOf(Triple("name", Operators.EQ, it.second)), alias = "device"),
//                    null,
//                    null,
//                    Step(HasTS),
//                    Step(Measurement, alias = "meas")
//                )
//            )
//            val result = query(g, pattern, where = listOf(Compare("env", "meas", "location", Operators.ST_CONTAINS)), by = listOf(Aggregate("device", "name"), Aggregate("env","name")))
//
//            kotlin.test.assertEquals(it.first, result.size)
//        }
//
//    }

}