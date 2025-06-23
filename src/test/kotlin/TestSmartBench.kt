import com.sun.jdi.IntegerValue
import it.unibo.evaluation.dtgraph.SmartBenchDataLoader
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Direction
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Labels.*
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.interfaces.labelFromString
import it.unibo.graph.query.*
import it.unibo.graph.structure.CustomGraph
import it.unibo.graph.utils.saveResults
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.rocksdb.Env
import java.io.File
import kotlin.system.measureTimeMillis
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestSmartBench {

    private lateinit var graph: Graph

//    private fun getFolderSize(folder: File): Long {
//        var size: Long = 0
//        if (folder.exists() && folder.isDirectory) {
//            folder.listFiles()?.forEach { file ->
//                size += if (file.isFile) {
//                    file.length()
//                } else {
//                    getFolderSize(file)
//                }
//            }
//        }
//        return size
//    }


    @BeforeAll
    fun setup() {
        val dataset = "smartbench"
        val size = "small"
        val data: List<String> = listOf(
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
            //"dataset/$dataset/$size/observation.json"
        )
        graph = CustomGraph(MemoryGraph())
        graph.tsm = AsterixDBTSM.createDefault(graph)
        graph.clear()
        graph.getTSM().clear()

        val loader = SmartBenchDataLoader(graph)
        val executionTime = measureTimeMillis {
            loader.loadData(data)
        }
        println("Should be done")
        println("Loaded ${graph.getNodes().size} verticles")
        println("Loaded ${graph.getEdges().size} edges")
        println("Loaded ${graph.getProps().size} props")
        println("Ingestion Time: ${executionTime / 1000} s")
    }

    /*
     * EnvironmentCoverage(L, 𝜏), where L is a loca-
     * tion, and 𝜏 is a measurement type: lists all agents that
     * can generate measurements of a given type 𝜏 that can
     * cover the environments/locations specified in L.
     */
    @Test
    fun environmentCoverage() {
        val tau = Temperature
        val infrastructureId = "3042"

        val pattern = listOf(
            listOf(
                Step(Sensor, alias = "s1"),
                Step(hasCoverage),
                Step(Infrastructure, listOf(Filter("name", Operators.EQ, infrastructureId)), alias = "Environment")
            ),
            listOf(
                Step(Sensor, alias = "s2"),
                Step(labelFromString("has$tau")),
                Step(tau)
            )
        )

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, listOf(Filter("name", Operators.EQ, infrastructureId)), alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN ),
            Step(Sensor, alias="device"),
            Step(labelFromString("has$tau")),
            Step(tau)
        )

        val spatialPattern = listOf(
            listOf(Step(Infrastructure, listOf(Filter("name", Operators.EQ, infrastructureId)), alias = "targetLocation")),
            listOf(
                Step(Sensor, alias = "s1"),
                Step(labelFromString("has$tau")),
                Step(tau),
                Step(HasTS),
                Step(tau, alias = "Measurement")
            )
        )

        val spatialResult: List<Any>
        val semanticResult: List<Any>
        val edgesDirectionResult: List<Any>

        val edgesDirectionTime = measureTimeMillis {
            edgesDirectionResult = search(graph, edgesDirectionPattern, timeaware = false)
        }

        // v.1
        val semanticQueryTime = measureTimeMillis {
            semanticResult =
                query(graph, pattern, where = listOf(Compare("s1", "s2", "name", Operators.EQ)), timeaware = false)
        }

        val spatialQueryTime = measureTimeMillis {
            // v.2
            spatialResult = query(
                graph, spatialPattern,
                where = listOf(Compare("targetLocation", "Measurement", "location", Operators.ST_INTERSECTS)),
                by = listOf(Aggregate("s1", "name")),
                timeaware = false
            )
        }

        println("--- EnvironmentCoverage execution times ---")
        println("Semantic query time $semanticQueryTime ms and returned ${semanticResult.size}  items")
        println("Spatial query time $spatialQueryTime ms and returned ${spatialResult.size}  items")
        println("Edges direction query time $edgesDirectionTime ms and returned ${edgesDirectionResult.size}  items")

    }

    /*
     *  EnvironmentsAggregate(𝜖, 𝜏, 𝑡𝑎 , 𝑡𝑏 ): List, for each Environment during the period [𝑡𝑎, 𝑡𝑏 [, the average value of type 𝜏 during
     * the period [𝑡𝑎, 𝑡𝑏 [ for each agent.
     */
    @Test
    fun environmentAggregate() {
        val tau = Temperature
        val tA = 1510700400000L // 15/11/2017 00:00:00
        val tB = 1511564400000L // 25/12/2017 00:00:00

        // v.1 Following graph edges
        val pattern = listOf(
            listOf(
                Step(Sensor, alias = "Device"),
                null,
                Step(Infrastructure, alias = "Environment")
            ),
            listOf(
                Step(Sensor, alias = "Device2"),
                null,
                Step(tau),
                Step(HasTS),
                Step(tau, alias = "Measurement")
            )
        )
        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            Step(tau),
            Step(HasTS),
            Step(tau, alias = "Measurement")
        )

        val spatialPattern = listOf(
            listOf(
                Step(Infrastructure, alias = "Environment")
            ),
            listOf(
                Step(Sensor, alias = "Device"),
                null,
                Step(tau),
                Step(HasTS),
                Step(tau, alias = "Measurement")
            )
        )

        val spatialResult: List<Any>
        val semanticResult: List<Any>
        val edgesDirectionResult: List<Any>

        val spatialResultTime = measureTimeMillis {
            spatialResult = query(
                graph, spatialPattern,
                where = listOf(Compare("Environment", "Measurement", "location", Operators.ST_INTERSECTS)),
                by = listOf(
                    Aggregate("Environment","name"),
                    Aggregate("Device", "name"),
                    Aggregate("Measurement","value", AggOperator.AVG)
                ),
                from = tA, to = tB, timeaware = true
            )
        }

        val edgesResultTime = measureTimeMillis {
            edgesDirectionResult = query(
                graph, edgesDirectionPattern,
                by = listOf(
                    Aggregate("Environment","name"),
                    Aggregate("Device", "name"),
                    Aggregate("Measurement","value", AggOperator.AVG)
                ),
                from = tA, to = tB, timeaware = true
            )
        }

        val semanticQueryTime = measureTimeMillis {
            semanticResult = query(
                graph, pattern,
                where = listOf(Compare("Device", "Device2", "name", Operators.EQ)),
                by = listOf(
                    Aggregate("Environment","name"),
                    Aggregate("Device", "name"),
                    Aggregate("Measurement","value", AggOperator.AVG),
                ),
                from = tA, to = tB, timeaware = true
            )
        }

        println("--- EnvironmentAggregate execution times ---")
        println("Semantic query time $semanticQueryTime ms and returned ${semanticResult.size} items")
        println("Spatial query time $spatialResultTime ms and returned ${spatialResult.size} items")
        println("Edges direction time $edgesResultTime ms and returned ${edgesDirectionResult.size} items")
    }

    /*
     * MaintenanceOwners(𝜏, alpha): List all owners of devices that measured took measurements
     * of type 𝜏 above a threshold alpha during the period [𝑡𝑎, 𝑡𝑏 [
    */ //TODO
    @Test
    fun MaintenanceOwners() {
        val tA = 1510700400000L // 15/11/2017 00:00:00 - 24785
        val tB = 1511564400000L // 25/12/2017 00:00:00 -
        val minTemp = 65L

        val edgesDirectionPattern = listOf(
            listOf(
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN ),
                Step(Sensor, alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(Temperature, properties = listOf(Filter("value",Operators.GTE, minTemp)), alias = "Measurement")
            ),
            listOf(
                Step(Sensor, alias = "Device2"),
                Step(hasOwner),
                Step(User, alias = "Owner")
            )
        )

        val edgesDirectionResult : List<Any>
        val edgesDirectionQueryTime = measureTimeMillis {
            edgesDirectionResult = query(
                graph, edgesDirectionPattern,
                where = listOf(Compare("Device","Device2","name",Operators.EQ)),
                by = listOf(
                    Aggregate("Device", "name"),
                    Aggregate("Environment","name"),
                    Aggregate("Owner", "emailId"),
                ),
                from = tA,
                to = tB,
                timeaware = true
            )
        }
    //TODO: NON CREDO DI POTER FARE MULTIJOIN
//        val spatialPattern = listOf(
//            listOf(
//                Step(Infrastructure, alias = "Environment")
//            ),
//            listOf(
//                Step(Sensor, alias = "Device"),
//                null,
//                Step(Temperature),
//                Step(HasTS),
//                Step(Temperature, alias = "Measurement")
//            )
//        )
//
//        val spatialQueryTime = measureTimeMillis {
//            spatialResult = query(
//                graph, spatialPattern,
//                where = listOf(Compare("Environment", "Measurement", "location", Operators.ST_INTERSECTS)),
//                by = listOf(
//                    Aggregate("Device", "name"),
//                    Aggregate("Environment", "name"),
//                    Aggregate("Measurement", "value", AggOperator.AVG)
//                ),
//                from = tA,
//                to = tB,
//                timeaware = true
//            )//from = 0, to = 5)
//          }

        println("--- MaintenanceOwners execution times ---")
//        println("Semantic query time $semanticQueryTime ms and returned ${semanticResult.size} items")
//        println("Spatial query time $spatialQueryTime ms and returned ${spatialResult.size} items")
        println("Edges direction time $edgesDirectionQueryTime ms and returned ${edgesDirectionResult.size} items")

    }

    /*
     * EnvironmentAlert: List the environments that have had a an average temperature > 20 degrees during
     * the period [𝑡𝑎, 𝑡𝑏 [.
     */
    @Test
    fun EnvironmentOutlier(){

        //TODO: Fixa average

        val minTemp = 60L
        val tA= 1510095600000L //08/11/2017 00:00:00
        val tB = 1510106400000L //08/11/2017 03:00:00

        val pattern = listOf(
            listOf(
                Step(Sensor, alias = "Device"),
                null,
                Step(Infrastructure, alias = "Environment")
            ),
            listOf(
                Step(Sensor, alias = "Device2"),
                null,
                null,
                Step(HasTS),
                Step(Temperature, properties = listOf(Filter("value", Operators.GTE, minTemp)), alias = "Measurement")
            )
        )

        val semanticResult : List<Any>
        val spatialResult : List<Any>
        val edgesDirectionResult : List<Any>

        val semanticQueryTime = measureTimeMillis {
            semanticResult = query(graph, pattern,
                where= listOf(Compare("Device","Device2","name",Operators.EQ)),
                by=listOf(Aggregate("Environment","name")),
                from = tA,
                to = tB,
                timeaware = true
            )
        }

        val spatialPattern = listOf(
            listOf(Step(Infrastructure, alias = "Environment")),
            listOf(
                Step(Sensor, alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(Temperature, properties = listOf(Filter("value", Operators.GTE, minTemp)), alias = "Measurement")

            )
        )

        val spatialQueryTime = measureTimeMillis {
            spatialResult = query(graph, spatialPattern,
                where=listOf(Compare("Environment","Measurement","location",Operators.ST_INTERSECTS)),
                by=listOf(Aggregate("Environment","name")),//, Aggregate("Measurement","value",AggOperator.AVG))
                from = tA,
                to = tB,
                timeaware = true
            )
        }

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            null,
            Step(HasTS),
            Step(Temperature, properties = listOf(Filter("value", Operators.GTE, minTemp)), alias = "Measurement")
        )

        val edgesDirectionQueryTime = measureTimeMillis {
            edgesDirectionResult = query(graph, edgesDirectionPattern,
                where=listOf(Compare("Environment","Measurement","location",Operators.ST_INTERSECTS)),
                by=listOf(Aggregate("Environment","name")),//, Aggregate("Measurement","value",AggOperator.AVG))
                from = tA,
                to = tB,
                timeaware = true
            )
        }

        println("--- EnvironmentTemperature execution times ---")
        println("Semantic query time $semanticQueryTime ms and returned ${semanticResult.size} items")
        println("Spatial query time $spatialQueryTime ms and returned ${spatialResult.size} items")
        println("Edges direction time $edgesDirectionQueryTime ms and returned ${edgesDirectionResult.size} items")

    }

    /*
     * AgentOutlier: List the max value measured for each agent in each environment
     */
    @Test
    fun AgentOutlier() {
        val tau = Temperature
        val tA = 1510095600000 // 08/11/2017 00:00:00
        val tB = 1518044400000 // 08/02/2018 00:00:00

        val semanticPattern = listOf(
            listOf(
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN),
                Step(Sensor, alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(tau, alias = "Measurement")
            )
        )

        val spatialPattern = listOf(
            listOf(
                Step(Infrastructure, alias = "Environment")
            ),
            listOf(
                Step(Sensor, alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(tau, alias = "Measurement")
            )
        )

        val semanticResult : List<Any>
        val spatialResult : List<Any>

        val semanticQueryTime = measureTimeMillis {
            semanticResult = query(graph, semanticPattern,
                by = listOf(Aggregate("Device","name"), Aggregate("Environment","name"), Aggregate("Measurement","value",AggOperator.MAX)),
                from = tA,
                to = tB,
                timeaware = true)
        }

        val spatialQueryTime = measureTimeMillis {
            spatialResult = query(graph, spatialPattern,
                where = listOf(Compare("Environment","Measurement","location",Operators.ST_INTERSECTS)),
                by = listOf(Aggregate("Device","name"), Aggregate("Environment","name"), Aggregate("Measurement","value",AggOperator.MAX)),
                from = tA,
                to = tB,
                timeaware = true)
        }

        println("--- AgentOutlier execution times ---")
        println("Semantic query time $semanticQueryTime ms and returned ${semanticResult.size} items")
        println("Spatial query time $spatialQueryTime ms and returned ${spatialResult.size} items")
    }

    /*
     * AgentHistory(𝐴): where 𝐴 is a set of agents. For each
     * 𝛼 ∈ 𝐴, list all environments 𝜖 for which 𝛼 generated
     * measurements in.
     */
    @Test
    fun agentHistory() {
        val devices = listOf("Thermometer3")

        var semanticEntities = 0
        var spatialEntities = 0
        var edgesDirectionEntitites = 0

        var semanticResult: List<Any>
        var spatialResult: List<Any>
        var edgesDirectionResult: List<Any>

        var semanticQueryTime: Long = 0
        var spatialQueryTime: Long = 0
        var edgesDirectionQueryTime: Long = 0

        devices.forEach {
            val traversalPattern = listOf(
                listOf(
                    Step(Sensor, listOf(Filter("name", Operators.EQ, it)), alias = "Device"),
                    null,
                    Step(Infrastructure, alias = "Environment")
                ),
                listOf(
                    Step(Sensor, listOf(Filter("name", Operators.EQ, it)), alias = "Device2"),
                    null,
                    null,
                    Step(HasTS),
                    Step(Temperature, alias = "Measurement")
                )
            )
            val edgesDirectionPattern = listOf(
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN),
                Step(Sensor, listOf(Filter("name", Operators.EQ, it)),  alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(Temperature, alias = "Measurement")
            )

            edgesDirectionQueryTime += measureTimeMillis {
                edgesDirectionResult = query(
                    graph, edgesDirectionPattern,
                    by = listOf(Aggregate("Device", "name"), Aggregate("Environment", "name")), timeaware = true
                )
            }
            edgesDirectionEntitites += edgesDirectionResult.size


            semanticQueryTime += measureTimeMillis {
                semanticResult = query(
                    graph, traversalPattern,
                    where = listOf(Compare("Device", "Device2", "name", Operators.EQ)),
                    by = listOf(Aggregate("Device", "name"), Aggregate("Environment", "name")), timeaware = true
                )
            }
            semanticEntities += semanticResult.size
        }


        devices.forEach {
            val spatialPattern = listOf(
                listOf(Step(Infrastructure, alias = "Environment")),
                listOf(
                    Step(Sensor, listOf(Filter("name", Operators.EQ, it)), alias = "Device"),
                    null,
                    null,
                    Step(HasTS),
                    Step(Temperature, alias = "Measurement")
                )
            )
            spatialQueryTime += measureTimeMillis {
                spatialResult = query(
                    graph, spatialPattern,
                    where = listOf(Compare("Environment", "Measurement", "location", Operators.ST_INTERSECTS)),
                    by = listOf(Aggregate("Device", "name"), Aggregate("Environment", "name"))
                )
            }
            spatialEntities += spatialResult.size

        }
        println("--- AgentHistory execution times ---")
        println("Semantic query time $semanticQueryTime ms and returned $semanticEntities items")
        println("Spatial query time $spatialQueryTime ms and returned $spatialEntities items")
        println("Edges direction time $edgesDirectionQueryTime ms and returned $edgesDirectionEntitites items")
    }

}

//        // M -> A -> E
//        /*
//     * CurrenteAgentLocation(𝑡𝑎 , 𝑡𝑏 ): list the current location
//     * for the agents that performed measurements during
//     * [𝑡𝑎, 𝑡𝑏 [.
//     */
//        /**
//         *     // v.1 Following path traversal
//         *
//         *
//         *  MATCH    (device: Device),
//         *          (nowEnv:AgriParcel) - [:HasDevice] - (nowDevice: Device)
//         *  WHERE device.name IN (
//         *          MATCH (oldDevice:Device) - [] - () - [:HasTS] - ()
//         *          VALID FROM 0 TO 2
//         *          RETURN oldDevice.name)
//         *  AND device.name == nowDevice.name
//         *  RETURN nowDevice, nowEnv
//         *
//         *
//         *     // v.2 Following spatial contains
//         *
//         *  MATCH    (device: Device),
//         *          (nowEnv:AgriParcel)
//         *  WHERE device.name IN (
//         *          MATCH (oldDevice:Device) - [] - () - [:HasTS] - ()
//         *          VALID FROM 0 TO 2
//         *          RETURN oldDevice.name)
//         *  AND ST_INTERSECTS(nowEnv, device)
//         *  RETURN nowDevice, nowEnv
//         */
//        @Test
//        fun currentAgentLocation() {
//            val tA = Long.MIN_VALUE
//            val tB = Long.MAX_VALUE
//
//
//            val historicalPattern = listOf(
//                Step(Sensor, alias = "oldDevice"),
//                null,
//                null,
//                Step(HasTS),
//                null
//            )
//
//            val devicesInTime: List<Any>
//
//            // TODO: FIXA FROM E TO
//            val devicesInTimeQueryTime = measureTimeMillis {
//                devicesInTime = query(
//                    graph,
//                    historicalPattern,
//                    by = listOf(Aggregate("oldDevice", "name")),
//                    timeaware = true
//                )//from = 0, to = 2, timeaware = true)
//            }
//
//            //kotlin.test.assertEquals(resultMap.size, devicesInTime.size)
//
//            if (devicesInTime.isEmpty()) {
//                throw Exception()
//            }
//
//            var semanticQueryTime = 0L
//            var spatialQueryTime = 0L
//            var semanticEntities = 0
//            var spatialEntities = 0
//
//            devicesInTime.forEach {
//                val semanticResult: List<Any>
//                val spatialResult: List<Any>
//
//                val nowPattern = listOf(
//                    Step(Sensor, listOf(Filter("name", Operators.EQ, it)), alias = "nowDevice"),
//                    null,
//                    Step(Infrastructure, alias = "env")
//
//                )
//                val nowSpatialPattern = listOf(
//                    listOf(Step(Infrastructure, alias = "parcel")),
//                    listOf(Step(Sensor, listOf(Filter("name", Operators.EQ, it)), alias = "nowDevice")),
//                )
//
//                // TODO: FIXA FROM E TO
//                semanticQueryTime += measureTimeMillis {
//                    semanticResult = query(
//                        graph, nowPattern, by = listOf(
//                            Aggregate("nowDevice", "name"), Aggregate("env", "name")
//                        )
//                    )//, from = 4, to = Long.MAX_VALUE)
//                }
//                semanticEntities += semanticResult.size
//
//                // TODO: FIXA FROM E TO
//                spatialQueryTime += measureTimeMillis {
//                    spatialResult = query(
//                        graph,
//                        nowSpatialPattern,
//                        where = listOf(Compare("parcel", "nowDevice", "location", Operators.ST_INTERSECTS)),
//                        by = listOf(
//                            Aggregate("nowDevice", "name"), Aggregate("parcel", "name")
//                        )
//                    ) //,from = 4, to = Long.MAX_VALUE)
//                }
//                spatialEntities += spatialResult.size
//
//                //kotlin.test.assertEquals(resultMap[it].toString(),  (actualLocation as List<List<Any>>).firstOrNull()?.getOrNull(1)?.toString() ?: "")
//                //kotlin.test.assertEquals(resultMap[it].toString(),  (actualLocationBySpatial as List<List<Any>>).firstOrNull()?.getOrNull(1)?.toString() ?: "")
//            }
//
//            println("--- CurrentAgentLocation execution times ---")
//            println("Semantic query time ${semanticQueryTime + devicesInTimeQueryTime} ms and returned $semanticEntities items")
//            println("Spatial query time ${spatialQueryTime + devicesInTimeQueryTime} ms and returned $spatialEntities items")
//
//        }

        // M -> E -> A
        /*
     * ActiveAgents(𝑡𝑎 , 𝑡𝑏 ): list all the agents currently in the
     * environments in which were performed some measurements in the period [𝑡𝑎, 𝑡𝑏 [
     */

//        @Test
//        fun activeAgents() {
//            val resultMap = mapOf(
//                "Errano T1" to listOf("Errano T1 MoistureDevice"),
//                "Errano T2" to listOf("Errano T2 MoistureDevice", "Errano Drone"),
//            )
//            val tA = Long.MIN_VALUE
//            val tB = Long.MAX_VALUE
//
//            // Adding a new parcel that should not be considered
//            val erranoT0 = graph.addNode(AgriParcel)
//            graph.addProperty(erranoT0.id, "location", T0_LOCATION, PropType.GEOMETRY)
//            val t0Device = graph.addNode(Device)
//            graph.addEdge(HasDevice, erranoT0.id, t0Device.id)
//
//            val t0TS = graph.getTSM().addTS()
//            t0TS.add(Measurement, 4, 4, "POINT (11.798998 44.235024)")
//
//            val t0Hum = graph.addNode(Humidity, value = t0TS.getTSId())
//            graph.addEdge(HasHumidity, t0Device.id, t0Hum.id)
//
//            val oldMeasurementsLocationPattern = listOf(
//                listOf(
//                    Step(Infrastructure, alias = "Environment")
//                ),
//                listOf(
//                    Step(Sensor),
//                    null,
//                    null,
//                    Step(HasTS),
//                    Step(Temperature, alias = "Measurement")
//                ),
//            )
//
//            val oldMeasurementsLocations = query(
//                graph, oldMeasurementsLocationPattern,
//                where = listOf(
//                    Compare(
//                        "Environment", "Measurement", "location",
//                        Operators.ST_INTERSECTS
//                    )
//                ), by = listOf(Aggregate("Environment", "name"))
//            ) //, from = tA, to = tB, timeaware = true)
//
//
//            oldMeasurementsLocations.forEach {
//                val activeAgentsPattern = listOf(
//                    Step(Sensor, alias = "Device"),
//                    null,
//                    Step(Infrastructure, listOf(Filter("name", Operators.EQ, it.toString())), alias = "Environment"),
//                )
//
//                val activeAgentsSpatialPattern = listOf(
//                    listOf(
//                        Step(
//                            Infrastructure,
//                            listOf(Filter("name", Operators.EQ, it.toString())),
//                            alias = "Environment"
//                        )
//                    ),
//                    listOf(Step(Sensor, alias = "Device"))
//                )
//
//                //TODO: FIXA QUESTO TO E FROM
//                val activeAgents = query(
//                    graph,
//                    activeAgentsPattern,
//                    by = listOf(Aggregate("Environment", "name"), Aggregate("Device", "name"))
//                )//, from = 4)
//
//                //TODO: FIXA QUESTO TO E FROM
//                val activeSpatialAgents = query(
//                    graph, activeAgentsSpatialPattern, where = listOf(
//                        Compare(
//                            "Environment", "Device", "location",
//                            Operators.ST_INTERSECTS
//                        )
//                    ), by = listOf(Aggregate("Environment", "name"), Aggregate("Device", "name"))
//                )//, from = 4)
//
//                var result = (activeAgents as List<List<Any>>)
//                    .filter { it.isNotEmpty() }
//                    .groupBy({ it[0] }, { it.drop(1) })
//                    .mapValues { it.value.flatten() }
//
//
//                result = (activeSpatialAgents as List<List<Any>>)
//                    .filter { it.isNotEmpty() }
//                    .groupBy({ it[0] }, { it.drop(1) })
//                    .mapValues { it.value.flatten() }
//
//            }
//
//        }

