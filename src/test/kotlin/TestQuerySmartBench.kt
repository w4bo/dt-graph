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

    val resultFolder = "results/dt_graph/"

    // Test params set through env variables
    private val dataset = System.getenv("DATASET") ?: "smartbench"
    private val testIterations = System.getenv("QUERY_ITERATIONS")?.toInt() ?: 1
    private val size = System.getenv("DATASET_SIZE") ?: "small"


    var uuid = UUID.randomUUID()

    private fun logQueryResult(queryName: String, queryType:String, queryTime: Long, numEntities: Int) {
        logger.info("$queryName - $queryType executed in $queryTime ms and returned $numEntities items")
        val outputDir = File("$resultFolder/query_evaluation/")
        if (!outputDir.exists()) outputDir.mkdirs()

        val file = File(outputDir, "statistics.csv")
        val writeHeader = !file.exists()

        file.appendText(buildString {
            if (writeHeader) append("test_id,model,dataset,datasetSize,threads,queryName,queryType,elapsedTime,numEntities,numMachines\n")
            append("${uuid},stgraph,$dataset,$size,$LIMIT,$queryName,$queryType,$queryTime,$numEntities,${System.getenv("DEFAULT_NC_POOL")?.toString()?.split(',')?.size ?: 1}\n")
        })
    }

    private lateinit var graph: Graph

    @BeforeAll
    fun setup() {
        graph = MemoryGraphACID()
        graph = MemoryGraphACID.readFromDisk() // Reload from disk
        val tsm = AsterixDBTSM.createDefault(graph)
        graph.tsm = tsm

        logger.info("Loaded ${graph.getNodes().size} vertexes")
        logger.info("Loaded ${graph.getEdges().size} edges")
        logger.info("Loaded ${graph.getProps().size} props")
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

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, listOf(Filter("id", Operators.EQ, infrastructureId)), alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN ),
            Step(Sensor, alias="device"),
            Step(labelFromString("has$tau")),
            Step(tau)
        )

        var edgesDirectionResult: List<Any> = listOf()
        var edgesDirectionTime : Long = 10L

        edgesDirectionTime = measureTimeMillis {
            edgesDirectionResult = query(graph,
                edgesDirectionPattern,
                by = listOf(Aggregate("Environment", "id"), Aggregate("device","id")), timeaware = false)
        }

        logger.info("--- EnvironmentCoverage execution times ---")
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

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            Step(tau),
            Step(HasTS),
            Step(tau, alias = "Measurement")
        )

        val edgesDirectionResult: List<Any>


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


        logger.info("--- EnvironmentAggregate execution times ---")
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

        val edgesDirectionQueryTime = measureTimeMillis {
            edgesDirectionResult = search(
                graph, simplePattern,
                by = listOf(
                    Aggregate("Device", "id"),
                    Aggregate("Environment","id"),
                    Aggregate("Owner","id")
                ),
                from = tA,
                to = tB,
                timeaware = true
            )
        }

        logger.info("--- MaintenanceOwners execution times ---")
        logQueryResult("MaintenanceOwners", "edgesDirection", edgesDirectionQueryTime, edgesDirectionResult.size)

    }

    /*
     * EnvironmentAlert: List the environments that have had a an average temperature > 20 degrees during
     * the period [𝑡𝑎, 𝑡𝑏 [.
     */
    fun environmentOutlier(){

        //TODO: Fixa average

        val minTemp = 60L
        val tA = 1510700400000L // 15/11/2017 00:00:00 - 24785
        val tB = 1511564400000L // 25/12/2017 00:00:00 -

        val edgesDirectionResult : List<Any>

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
        logQueryResult("EnvironmentOutlier", "edgesDirection", edgesDirectionQueryTime, edgesDirectionResult.size)
    }

    /*
     * AgentOutlier: List the max value measured for each agent in each environment
     */
    fun agentOutlier() {
        val tau = Temperature
        val tA = 1510095600000 // 08/11/2017 00:00:00
        val tB = 1516370586000 // 19/01/2018 00:00:00

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            null,
            Step(HasTS),
            Step(Temperature, alias = "Measurement")
        )

        val edgesDirectionResult : List<Any>

        val edgesDirectionTime = measureTimeMillis {
            edgesDirectionResult = query(graph, edgesDirectionPattern,
                by = listOf(Aggregate("Device","id"), Aggregate("Environment","id"), Aggregate("Measurement","value",AggOperator.MAX)),
                from = tA,
                to = tB,
                timeaware = true)
        }

        logger.info("--- AgentOutlier execution times ---")
        logQueryResult("AgentOutlier", "edgesDirection", edgesDirectionTime, edgesDirectionResult.size)

    }

    /*
     * AgentHistory(𝐴): where 𝐴 is a set of agents. For each
     * 𝛼 ∈ 𝐴, list all environments 𝜖 for which 𝛼 generated
     * measurements in.
     */
    fun agentHistory() {
        val devices = listOf("thermometer3")

        var edgesDirectionEntitites = 0
        var edgesDirectionResult: List<Any>
        var edgesDirectionQueryTime: Long = 0

        devices.forEach {
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
        }


        logger.info("--- AgentHistory execution times ---")
        logQueryResult("AgentHistory", "edgesDirection", edgesDirectionQueryTime, edgesDirectionEntitites)
    }

    @Test
    fun runAllQueriesNTimes() {
        repeat(testIterations) { i ->
            uuid = UUID.randomUUID()
            logger.info("\n=== RUN QUERY ITERATION #${i + 1} ===")

            fun safeRun(name: String, block: () -> Unit) {
                try {
                    block()
                } catch (e: Exception) {
                    logger.error("Query $name failed in iteration #${i + 1}: ${e.message}")
                }
            }

            safeRun("environmentCoverage") { environmentCoverage() }
            safeRun("environmentAggregate") { environmentAggregate() }
            safeRun("maintenanceOwners") { maintenanceOwners() }
            safeRun("environmentOutlier") { environmentOutlier() }
            safeRun("agentOutlier") { agentOutlier() }
            safeRun("agentHistory") { agentHistory() }
        }
    }
}
