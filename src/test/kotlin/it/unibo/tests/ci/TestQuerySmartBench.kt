package it.unibo.tests.ci

import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Direction
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Labels.*
import it.unibo.graph.interfaces.labelFromString
import it.unibo.graph.query.*
import it.unibo.graph.utils.LIMIT
import it.unibo.graph.utils.TimeRange
import it.unibo.graph.utils.loadTemporalRanges
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import java.io.File
import java.util.*
import kotlin.system.measureTimeMillis
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestQuerySmartBench {
    private val logger = LoggerFactory.getLogger(TestQuerySmartBench::class.java)
    private val temporalConstraintMap = loadYamlAsMap("time_constraints.yaml")
    private val resultFolder = "results/dt_graph/"

    // Test params set through env variables
    private val dataset = System.getenv("DATASET") ?: "smartbench"
    private val testIterations = System.getenv("QUERY_ITERATIONS")?.toInt() ?: 1
    private val size = System.getenv("DATASET_SIZE") ?: "large"
    private val querySelectivity = System.getenv("QUERY_SELECTIVITY") ?: "equal"


    var uuid = UUID.randomUUID()

    private fun loadYamlAsMap(resourcePath: String): Map<String, Any> {
        val inputStream = this::class.java.classLoader.getResourceAsStream(resourcePath)
            ?: throw IllegalArgumentException("$resourcePath not found in classpath")
        val yamlText = inputStream.bufferedReader().use { it.readText() }
        return Yaml().load<Map<String, Any>>(yamlText) ?: emptyMap()
    }

    private fun logQueryResult(queryName: String, queryType:String, queryTime: Long, numEntities: Int, temporalRangeIndex: Int, iteration: Int) {
        logger.info("$queryName - $queryType executed in $queryTime ms and returned $numEntities items")
        val outputDir = File("$resultFolder/query_evaluation/")
        if (!outputDir.exists()) outputDir.mkdirs()

        val file = File(outputDir, "statistics.csv")
        val writeHeader = !file.exists()

        file.appendText(buildString {
            if (writeHeader) append("test_id,model,dataset,datasetSize,threads,queryName,queryType,elapsedTime,numEntities,numMachines,querySelectivity,temporalRangeIndex,iteration\n")
            append("${uuid},STGraph,$dataset,$size,$LIMIT,$queryName,$queryType,$queryTime,$numEntities,${System.getenv("DEFAULT_NC_POOL")?.split(',')?.size ?: 1},${querySelectivity},$temporalRangeIndex,$iteration\n")
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
     * EnvironmentCoverage(L, 𝜏), where L is a location,
     * and 𝜏 is a measurement type: lists all agents that
     * can generate measurements of a given type 𝜏 that can
     * cover the environments/locations specified in L.
     */
    fun environmentCoverage(temporalConstraintIndex: Int = 0, iteration: Int = 0) {
        val tau = Temperature
        val infrastructureId = "3042"

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, listOf(Filter("id", Operators.EQ, infrastructureId)), alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN ),
            Step(Sensor, alias="device"),
            Step(labelFromString("has$tau")),
            Step(tau)
        )

        var edgesDirectionResult: List<Any>

        val edgesDirectionTime : Long = measureTimeMillis {
            edgesDirectionResult = query(graph,
                edgesDirectionPattern,
                by = listOf(Aggregate("Environment", "id"), Aggregate("device","id")), timeaware = false)
        }

        logger.info("--- EnvironmentCoverage execution times ---")
        logQueryResult("EnvironmentCoverage","edgesDirection", edgesDirectionTime, edgesDirectionResult.size,temporalConstraintIndex, iteration)


    }

    /*
     *  EnvironmentsAggregate(𝜖, 𝜏, 𝑡𝑎 , 𝑡𝑏 ): List, for each Environment during the period [𝑡𝑎, 𝑡𝑏 [, the average value of type 𝜏 during
     * the period [𝑡𝑎, 𝑡𝑏 [ for each agent.
     */
    fun environmentAggregate(temporalConstraints: TimeRange, temporalRangeIndex: Int, iteration: Int = 0) {
        val tA = temporalConstraints.from
        val tB = temporalConstraints.to
        val tau = Temperature

        val edgesDirectionPattern = listOf(
            listOf(
                Step(Sensor, alias = "Sensor"),
                Step(hasOwner),
                Step(User, alias = "Owner"),
                EdgeStep(hasOwner, direction = Direction.IN ),
                Step(Platform, alias = "Platform"),
                Step(hasType_),
                Step(PlatformType, alias = "PlatformType"),
            ),
            listOf(
                Step(InfrastructureType),
                EdgeStep(hasType_, direction = Direction.IN ),
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN ),
                Step(Sensor, alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(tau, alias = "Measurement")
            ),
        )

        val edgesDirectionResult: List<Any>


        val edgesQueryTime = measureTimeMillis {
            edgesDirectionResult = query(
                graph, edgesDirectionPattern,
                where = listOf(Compare("Device","Sensor","id",Operators.EQ)),
                by = listOf(
                    Aggregate("PlatformType", "id"),
                    Aggregate("Owner", "id"),
                    Aggregate("Environment","id"),
                    Aggregate("Measurement","value", AggOperator.AVG)
                ),
                from = tA, to = tB, timeaware = true
            )
        }


        logger.info("--- EnvironmentAggregate execution times ---")
        logQueryResult("EnvironmentAggregate", "edgesDirection", edgesQueryTime, edgesDirectionResult.size,temporalRangeIndex, iteration)
    }

    /*
     * MaintenanceOwners(𝜏, alpha): List all owners of devices that measured took measurements
     * of type 𝜏 above a threshold alpha during the period [𝑡𝑎, 𝑡𝑏 [
    */ //TODO
    fun maintenanceOwners(temporalConstraints: TimeRange, temporalRangeIndex: Int, iteration: Int = 0) {
        val tA = temporalConstraints.from
        val tB = temporalConstraints.to

        val minTemp = 65L

        val simplePattern = listOf(
            listOf(
                Step(Infrastructure, alias = "Environment"),
                EdgeStep(hasCoverage, direction = Direction.IN ),
                Step(Sensor, alias = "Device"),
                null,
                null,
                Step(HasTS),
                Step(Temperature, properties = listOf(Filter("value",Operators.GTE, minTemp)))
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
                graph, simplePattern,
                where = listOf(Compare("Device","Device2","id",Operators.EQ)),
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
        logQueryResult("MaintenanceOwners", "edgesDirection", edgesDirectionQueryTime, edgesDirectionResult.size, temporalRangeIndex, iteration)

    }

    /*
     * EnvironmentAlert: List the environments that have had a an average temperature > 20 degrees during
     * the period [𝑡𝑎, 𝑡𝑏 [.
     */
    fun environmentAlert(temporalConstraints: TimeRange, temporalRangeIndex: Int, iteration: Int = 0) {
        val tA = temporalConstraints.from
        val tB = temporalConstraints.to

        val minTemp = 50.5


        var edgesDirectionResult : List<Any>

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            null,
            Step(HasTS),
            Step(Temperature, alias = "Measurement")
        )

        val edgesDirectionQueryTime = measureTimeMillis {
            edgesDirectionResult = query(graph, edgesDirectionPattern,
                where=listOf(Compare("Environment","Measurement","location",Operators.ST_INTERSECTS)),
                by=listOf(Aggregate("Environment","id"), Aggregate("Measurement","value",AggOperator.AVG)),
                from = tA,
                to = tB,
                timeaware = true
            )
        }
        edgesDirectionResult = edgesDirectionResult.filter { ((it as ArrayList<*>)[1]!! as Double) > minTemp }
        logger.info("--- EnvironmentOutlier execution times ---")
        logQueryResult("EnvironmentOutlier", "edgesDirection", edgesDirectionQueryTime, edgesDirectionResult.size, temporalRangeIndex, iteration)
    }

    /*
     * AgentOutlier: List the max value measured for each agent in each environment
     */
    fun agentOutlier(temporalConstraints: TimeRange, temporalRangeIndex: Int, iteration: Int = 0) {
        val tA = temporalConstraints.from
        val tB = temporalConstraints.to
        val tau = Temperature

        val edgesDirectionPattern = listOf(
            Step(Infrastructure, alias = "Environment"),
            EdgeStep(hasCoverage, direction = Direction.IN),
            Step(Sensor, alias = "Device"),
            null,
            null,
            Step(HasTS),
            Step(tau, alias = "Measurement")
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
        logQueryResult("AgentOutlier", "edgesDirection", edgesDirectionTime, edgesDirectionResult.size, temporalRangeIndex, iteration)
    }

    /*
     * AgentHistory(𝐴): where 𝐴 is a set of agents. For each
     * 𝛼 ∈ 𝐴, list all environments 𝜖 for which 𝛼 generated
     * measurements in.
     */
    fun agentHistory(temporalConstraintIndex: Int = 0, iteration: Int = 0) {
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
        logQueryResult("AgentHistory", "edgesDirection", edgesDirectionQueryTime, edgesDirectionEntitites, temporalConstraintIndex, iteration)
    }



    @Test
    fun runAllQueriesNTimes() {
        repeat(testIterations) { i ->
            logger.info("\n=== RUN QUERY ITERATION #${i + 1} ===")

            fun <K, V> safeRun(name: String, ranges: Map<K, V>, block: (V, Int) -> Unit) {
                ranges.values.withIndex().forEach { (index, range) ->
                    try {
                        block(range, index)
                    } catch (e: Exception) {
                        logger.error("Query $name failed in iteration #${index + 1}: ${e.message}")
                    }
                }
            }

            safeRun("EnvironmentCoverage", loadTemporalRanges(temporalConstraintMap, querySelectivity, "EnvironmentCoverage", size)) { temporalRange, index ->
                environmentCoverage(index, i)
            }

            safeRun("EnvironmentAggregate", loadTemporalRanges(temporalConstraintMap, querySelectivity, "EnvironmentAggregate", size)) { temporalRange, index ->
                environmentAggregate(temporalRange, index, i)
            }

            safeRun("MaintenanceOwners", loadTemporalRanges(temporalConstraintMap, querySelectivity, "MaintenanceOwners", size)) { temporalRange, index ->
                maintenanceOwners(temporalRange, index, i)
            }

            safeRun("EnvironmentOutlier", loadTemporalRanges(temporalConstraintMap, querySelectivity, "EnvironmentOutlier", size)) { temporalRange, index ->
                environmentAlert(temporalRange, index, i)
            }

            safeRun("AgentOutlier", loadTemporalRanges(temporalConstraintMap, querySelectivity, "AgentOutlier", size)) { temporalRange, index ->
                agentOutlier(temporalRange, index, i)
            }

            safeRun("AgentHistory", loadTemporalRanges(temporalConstraintMap, querySelectivity, "AgentHistory", size)) { temporalRange, index ->
                agentHistory(index, i)
            }
        }
    }
}
