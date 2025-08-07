import it.unibo.evaluation.dtgraph.SmartBenchDataLoader
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Direction
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Labels.*
import it.unibo.graph.interfaces.labelFromString
import it.unibo.graph.query.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import java.util.*
import java.io.File
import kotlin.system.measureTimeMillis
import kotlin.test.Test
import it.unibo.graph.utils.LIMIT
import org.slf4j.LoggerFactory

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestQuerySmartBench {
    private val logger = LoggerFactory.getLogger(TestQuerySmartBench::class.java)

    val resultFolder = props["smartbench_results_folder"] ?: "results/dt_graph/"

    // Test params set through env variables
    private val dataset = System.getenv("DATASET") ?: "smartbench"
    private val testIterations = System.getenv("QUERY_ITERATIONS")?.toInt() ?: 1
    private val size = System.getenv("DATASET_SIZE") ?: "small"


    var uuid = UUID.randomUUID()

    private fun logQueryResult(queryName: String, queryType:String, queryTime: Long, numEntities: Int) {
        println("$queryName - $queryType executed in $queryTime ms and returned $numEntities items")
        val outputDir = File("$resultFolder/query_evaluation/$dataset")
        if (!outputDir.exists()) outputDir.mkdirs()

        val file = File(outputDir, "statistics.csv")
        val writeHeader = !file.exists()

        file.appendText(buildString {
            if (writeHeader) append("test_id,model,datasetSize,threads,queryName,queryType,elapsedTime,numEntities\n")
            append("${uuid},dtgraph,$size,$LIMIT,$queryName,$queryType,$queryTime,$numEntities\n")
        })
    }

    private lateinit var graph: Graph

    @BeforeAll
    fun setup() {
        graph = MemoryGraphACID()
        graph = MemoryGraphACID.readFromDisk() // Reload from disk
        val tsm = AsterixDBTSM.createDefault(graph)
        graph.tsm = tsm

        println("Loaded ${graph.getNodes().size} vertexes")
        println("Loaded ${graph.getEdges().size} edges")
        println("Loaded ${graph.getProps().size} props")
    }

    /*
     * EnvironmentCoverage(L, 𝜏), where L is a loca-
     * tion, and 𝜏 is a measurement type: lists all agents that
     * can generate measurements of a given type 𝜏 that can
     * cover the environments/locations specified in L.
     */
    fun environmentCoverage() {
        val tau = Temperature
        val infrastructureId = "3042"

        val pattern = listOf(
            listOf(
                Step(Sensor, alias = "s1"),
                Step(hasCoverage),
                Step(Infrastructure, listOf(Filter("id", Operators.EQ, infrastructureId)), alias = "Environment")
            ),
            listOf(
                Step(Sensor, alias = "s2"),
                Step(labelFromString("has$tau")),
                Step(tau)
            )
        )

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, listOf(Filter("id", Operators.EQ, infrastructureId)), alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN ),
            Step(Sensor, alias="device"),
            Step(labelFromString("has$tau")),
            Step(tau)
        )

        val spatialPattern = listOf(
            listOf(Step(Infrastructure, listOf(Filter("id", Operators.EQ, infrastructureId)), alias = "targetLocation")
            ),
            listOf(
                Step(Sensor, alias = "s1"),
                Step(labelFromString("has$tau")),
                Step(tau),
                Step(HasTS),
                Step(tau, alias = "Measurement")
            )
        )

        var spatialResult: List<Any> = listOf()
        var semanticResult: List<Any> = listOf()
        var edgesDirectionResult: List<Any> = listOf()
        var edgesDirectionTime : Long = 10L
        var semanticQueryTime : Long = 10L
        var spatialQueryTime : Long = 10L

        edgesDirectionTime = measureTimeMillis {
            edgesDirectionResult = query(graph,
                edgesDirectionPattern,
                by = listOf(Aggregate("Environment", "id"), Aggregate("device","id")), timeaware = false)
        }


//        semanticQueryTime = measureTimeMillis {
//            semanticResult =
//                query(graph, pattern, where = listOf(Compare("s1", "s2", "id", Operators.EQ)), timeaware = false)
//        }
//
//        // v.1
//        spatialQueryTime = measureTimeMillis {
//            // v.2
//            spatialResult = query(
//                graph, spatialPattern,
//                where = listOf(Compare("targetLocation", "Measurement", "location", Operators.ST_INTERSECTS)),
//                by = listOf(Aggregate("s1", "id")),
//                timeaware = false
//            )
//        }

        logger.info("--- EnvironmentCoverage execution times ---")
//        logQueryResult("EnvironmentCoverage","semantic", semanticQueryTime, semanticResult.size)
//        logQueryResult("EnvironmentCoverage", "spatial", spatialQueryTime, spatialResult.size)
        logQueryResult("EnvironmentCoverage","edgesDirection", edgesDirectionTime, edgesDirectionResult.size)


    }

    /*
     *  EnvironmentsAggregate(𝜖, 𝜏, 𝑡𝑎 , 𝑡𝑏 ): List, for each Environment during the period [𝑡𝑎, 𝑡𝑏 [, the average value of type 𝜏 during
     * the period [𝑡𝑎, 𝑡𝑏 [ for each agent.
     */
    fun environmentAggregate() {
        val tau = Temperature
        val tA = 1510700400000L // 15/11/2017 00:00:00
        val tB = 1511564400000L // 25/12/2017 00:00:00

        // v.1 Following graph edges
        val pattern = listOf(
            listOf(
                Step(Sensor, alias = "Device2"),
                null,
                Step(tau),
                Step(HasTS),
                Step(tau, alias = "Measurement")
            ),
            listOf(
                Step(Sensor, alias = "Device"),
                null,
                Step(Infrastructure, alias = "Environment")
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
                Step(Sensor, alias = "Device"),
                null,
                Step(tau),
                Step(HasTS),
                Step(tau, alias = "Measurement")
            ),
            listOf(
                Step(Infrastructure, alias = "Environment")
            )
        )

        val spatialResult: List<Any>
        val semanticResult: List<Any>
        val edgesDirectionResult: List<Any>

//        val spatialQueryTime = measureTimeMillis {
//            spatialResult = query(
//                graph, spatialPattern,
//                where = listOf(Compare("Environment", "Measurement", "location", Operators.ST_INTERSECTS)),
//                by = listOf(
//                    Aggregate("Environment","id"),
//                    Aggregate("Device", "id"),
//                    Aggregate("Measurement","value", AggOperator.AVG)
//                ),
//                from = tA, to = tB, timeaware = true
//            )
//        }

        val edgesQueryTime = measureTimeMillis {
             edgesDirectionResult = query(
                graph, edgesDirectionPattern,
                by = listOf(
                    Aggregate("Environment","id"),
                    Aggregate("Device", "id"),
                    Aggregate("Measurement","value", AggOperator.AVG)
                ),
                from = tA, to = tB, timeaware = true
            )
        }



//        val semanticQueryTime = measureTimeMillis {
//            semanticResult = query(
//                graph, pattern,
//                where = listOf(Compare("Device", "Device2", "id", Operators.EQ)),
//                by = listOf(
//                    Aggregate("Environment","id"),
//                    Aggregate("Device", "id"),
//                    Aggregate("Measurement","value", AggOperator.AVG),
//                ),
//                from = tA, to = tB, timeaware = true
//            )
//        }

        logger.info("--- EnvironmentAggregate execution times ---")
//        logQueryResult("EnvironmentAggregate","semantic", semanticQueryTime, semanticResult.size)
//        logQueryResult("EnvironmentAggregate","spatial", spatialQueryTime, spatialResult.size)
        logQueryResult("EnvironmentAggregate", "edgesDirection", edgesQueryTime, edgesDirectionResult.size)
    }

    /*
     * MaintenanceOwners(𝜏, alpha): List all owners of devices that measured took measurements
     * of type 𝜏 above a threshold alpha during the period [𝑡𝑎, 𝑡𝑏 [
    */ //TODO
    fun maintenanceOwners() {
        val tA = 1510700400000L // 15/11/2017 00:00:00
        val tB = 1514156400000L // 25/12/2017 00:00:00
        val minTemp = 65L

        val edgesDirectionPattern = listOf(
            listOf(
                Step(Sensor, alias = "Device2"),
                Step(hasOwner),
                Step(User, alias = "Owner")
            ),
            listOf(
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN ),
                Step(Sensor, alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(Temperature, properties = listOf(Filter("value",Operators.LTE, minTemp)))
            ),
        )
        val simplePattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN ),
            Step(Sensor, alias = "Device"),
            null,
            null,
            Step(HasTS),
            Step(Temperature, properties = listOf(Filter("value",Operators.GTE, minTemp)))
        )
        val edgesDirectionResult : List<Any>
//        val edgesDirectionQueryTime = measureTimeMillis {
//            edgesDirectionResult = query(
//                graph, edgesDirectionPattern,
//                where = listOf(Compare("Device","Device2","id",Operators.EQ)),
//                by = listOf(
//                    Aggregate("Device", "id"),
//                    Aggregate("Environment","id"),
//                    Aggregate("Owner", "emailId"),
//                ),
//                from = tA,
//                to = tB,
//                timeaware = true
//            )
//        }
        val edgesDirectionQueryTime = measureTimeMillis {
            edgesDirectionResult = search(
                graph, simplePattern,
                by = listOf(
                    Aggregate("Device", "id"),
                    Aggregate("Environment","id"),
                ),
                from = tA,
                to = tB,
                timeaware = true
            )
        }
        // TODO: NON CREDO DI POTER FARE MULTIJOIN
        val spatialPattern = listOf(
            listOf(
                Step(Infrastructure, alias = "Environment")
            ),
            listOf(
                Step(Sensor, alias = "Device"),
                null,
                Step(Temperature),
                Step(HasTS),
                Step(Temperature, alias = "Measurement")
            )
        )
//
        val spatialResult : List<Any>
//        val spatialQueryTime = measureTimeMillis {
//            spatialResult = query(
//                graph, spatialPattern,
//                where = listOf(Compare("Environment", "Measurement", "location", Operators.ST_INTERSECTS)),
//                by = listOf(
//                    Aggregate("Device", "id"),
//                    Aggregate("Environment", "id"),
//                    Aggregate("Measurement", "value", AggOperator.AVG)
//                ),
//                from = tA,
//                to = tB,
//                timeaware = true
//            )//from = 0, to = 5)
//          }

        logger.info("--- MaintenanceOwners execution times ---")
        //logQueryResult("MaintenanceOwners", "semantic", semanticQueryTime, semanticResult.size)
        //logQueryResult("MaintenanceOwners", "spatial", spatialQueryTime, spatialResult.size)
        logQueryResult("MaintenanceOwners", "edgesDirection", edgesDirectionQueryTime, edgesDirectionResult.size)

    }

    /*
     * EnvironmentAlert: List the environments that have had a an average temperature > 20 degrees during
     * the period [𝑡𝑎, 𝑡𝑏 [.
     */
    fun environmentOutlier(){

        //TODO: Fixa average

        val minTemp = 60L
//        val tA= 1510095600000L //08/11/2017 00:00:00
//        val tB = 1510106400000L //08/11/2017 03:00:00
        val tA = 1510700400000L // 15/11/2017 00:00:00 - 24785
        val tB = 1511564400000L // 25/12/2017 00:00:00 -

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

//        val semanticQueryTime = measureTimeMillis {
//            semanticResult = query(graph, pattern,
//                where= listOf(Compare("Device","Device2","id",Operators.EQ)),
//                by=listOf(Aggregate("Environment","id")),
//                from = tA,
//                to = tB,
//                timeaware = true
//            )
//        }

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

//        val spatialQueryTime = measureTimeMillis {
//            spatialResult = query(graph, spatialPattern,
//                where=listOf(Compare("Environment","Measurement","location",Operators.ST_INTERSECTS)),
//                by=listOf(Aggregate("Environment","id")),//, Aggregate("Measurement","value",AggOperator.AVG))
//                from = tA,
//                to = tB,
//                timeaware = true
//            )
//        }

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
                by=listOf(Aggregate("Environment","id")),//, Aggregate("Measurement","value",AggOperator.AVG))
                from = tA,
                to = tB,
                timeaware = true
            )
        }

        logger.info("--- EnvironmentOutlier execution times ---")

        //logQueryResult("EnvironmentOutlier", "semantic", semanticQueryTime, semanticResult.size)
        //logQueryResult("EnvironmentOutlier", "spatial",  spatialQueryTime, spatialResult.size)
        logQueryResult("EnvironmentOutlier", "edgesDirection", edgesDirectionQueryTime, edgesDirectionResult.size)
    }

    /*
     * AgentOutlier: List the max value measured for each agent in each environment
     */
    fun agentOutlier() {
        val tau = Temperature
        val tA = 1510095600000 // 08/11/2017 00:00:00
        val tB = 1516370586000 // 19/01/2018 00:00:00

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

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            null,
            Step(HasTS),
            Step(Temperature, alias = "Measurement")
        )

        val semanticResult : List<Any>
        val spatialResult : List<Any>
        val edgesDirectionResult : List<Any>

//        val semanticQueryTime = measureTimeMillis {
//            semanticResult = query(graph, semanticPattern,
//                by = listOf(Aggregate("Device","id"), Aggregate("Environment","id"), Aggregate("Measurement","value",AggOperator.MAX)),
//                from = tA,
//                to = tB,
//                timeaware = true)
//        }

//        val spatialQueryTime = measureTimeMillis {
//            spatialResult = query(graph, spatialPattern,
//                where = listOf(Compare("Environment","Measurement","location",Operators.ST_INTERSECTS)),
//                by = listOf(Aggregate("Device","id"), Aggregate("Environment","id"), Aggregate("Measurement","value",AggOperator.MAX)),
//                from = tA,
//                to = tB,
//                timeaware = true)
//        }

        val edgesDirectionTime = measureTimeMillis {
            edgesDirectionResult = query(graph, edgesDirectionPattern,
                by = listOf(Aggregate("Device","id"), Aggregate("Environment","id"), Aggregate("Measurement","value",AggOperator.MAX)),
                from = tA,
                to = tB,
                timeaware = true)
        }

        logger.info("--- AgentOutlier execution times ---")
        //logQueryResult("AgentOutlier", "semantic", semanticQueryTime, semanticResult.size)
        //logQueryResult("AgentOutlier", "spatial", spatialQueryTime, spatialResult.size)
        logQueryResult("AgentOutlier", "edgesDirection", edgesDirectionTime, edgesDirectionResult.size)

    }

    /*
     * AgentHistory(𝐴): where 𝐴 is a set of agents. For each
     * 𝛼 ∈ 𝐴, list all environments 𝜖 for which 𝛼 generated
     * measurements in.
     */
    fun agentHistory() {
        val devices = listOf("thermometer3")

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
                    Step(Sensor, listOf(Filter("id", Operators.EQ, it)), alias = "Device"),
                    null,
                    Step(Infrastructure, alias = "Environment")
                ),
                listOf(
                    Step(Sensor, listOf(Filter("id", Operators.EQ, it)), alias = "Device2"),
                    null,
                    null,
                    Step(HasTS),
                    Step(Temperature, alias = "Measurement")
                )
            )
            val edgesDirectionPattern = listOf(
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN),
                Step(Sensor, listOf(Filter("id", Operators.EQ, it)),  alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(Temperature, alias = "Measurement")
            )

            edgesDirectionQueryTime += measureTimeMillis {
                edgesDirectionResult = query(
                    graph, edgesDirectionPattern,
                    by = listOf(Aggregate("Device", "id"), Aggregate("Environment", "id")),
                    timeaware = true
                )
            }
            edgesDirectionEntitites += edgesDirectionResult.size


//            semanticQueryTime += measureTimeMillis {
//                semanticResult = query(
//                    graph, traversalPattern,
//                    where = listOf(Compare("Device", "Device2", "id", Operators.EQ)),
//                    by = listOf(Aggregate("Device", "id"), Aggregate("Environment", "id")),
//                    timeaware = true
//                )
//            }
//            semanticEntities += semanticResult.size
        }


//        devices.forEach {
//            val spatialPattern = listOf(
//                listOf(Step(Infrastructure, alias = "Environment")),
//                listOf(
//                    Step(Sensor, listOf(Filter("id", Operators.EQ, it)), alias = "Device"),
//                    null,
//                    null,
//                    Step(HasTS),
//                    Step(Temperature, alias = "Measurement")
//                )
//            )
//            spatialQueryTime += measureTimeMillis {
//                spatialResult = query(
//                    graph, spatialPattern,
//                    where = listOf(Compare("Environment", "Measurement", "location", Operators.ST_INTERSECTS)),
//                    by = listOf(Aggregate("Device", "id"), Aggregate("Environment", "id"))
//                )
//            }
//            spatialEntities += spatialResult.size
//
//        }

        logger.info("--- AgentHistory execution times ---")
//        logQueryResult("AgentHistory", "semantic", semanticQueryTime, semanticEntities)
//        logQueryResult("AgentHistory", "spatial", spatialQueryTime, spatialEntities)
        logQueryResult("AgentHistory", "edgesDirection", edgesDirectionQueryTime, edgesDirectionEntitites)
    }

    @Test
    fun runAllQueriesNTimes() {
        repeat(testIterations) { i ->
            uuid = UUID.randomUUID()
            logger.info("\n=== RUN QUERY ITERATION #${i + 1} ===")
            environmentCoverage()
            environmentAggregate()
            maintenanceOwners()
            environmentOutlier()
            agentOutlier()
            agentHistory()
        }
    }
}
