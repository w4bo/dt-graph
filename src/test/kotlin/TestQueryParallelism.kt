import it.unibo.ingestion.SmartBenchDataLoader
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.utils.loadProps
import kotlinx.coroutines.*
import org.json.JSONObject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import kotlin.system.measureTimeMillis
import kotlin.test.Test
import kotlin.test.fail


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestQueryParallelism {
    private lateinit var graph: Graph

    @BeforeAll
    fun setup() {
        val dataset = "smartbench"
        val size = "small"
        val data: List<String> = listOf(
            "datasets/dataset/$dataset/$size/group.json",
            "datasets/dataset/$dataset/$size/user.json",
            "datasets/dataset/$dataset/$size/platformType.json",
            "datasets/dataset/$dataset/$size/sensorType.json",
            "datasets/dataset/$dataset/$size/platform.json",
            "datasets/dataset/$dataset/$size/infrastructureType.json",
            "datasets/dataset/$dataset/$size/infrastructure.json",
            "datasets/dataset/$dataset/$size/sensor.json",
            "datasets/dataset/$dataset/$size/virtualSensorType.json",
            "datasets/dataset/$dataset/$size/virtualSensor.json",
            "datasets/dataset/$dataset/$size/semanticObservationType.json",
            //"dataset/$dataset/$size/semanticObservation.json",
            //"dataset/$dataset/$size/observation.json"
        )
        graph = MemoryGraphACID()
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

//    @Test
//    fun testAsterixParallelism(){
//        suspend fun query(id: Int, ccUrl: String, queryStatement: String): Int? = withContext(Dispatchers.IO) {
//            return@withContext try {
//                val connection = URL(ccUrl).openConnection() as HttpURLConnection
//                connection.requestMethod = "POST"
//                connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
//                connection.doOutput = true
//
//                val params = mapOf(
//                    "statement" to queryStatement,
//                    "pretty" to "true",
//                    "mode" to "immediate",
//                    "dataverse" to "Measurements_Dataverse"
//                )
//
//                val postData = params.entries.joinToString("&") {
//                    "${URLEncoder.encode(it.key, StandardCharsets.UTF_8.name())}=${
//                        URLEncoder.encode(
//                            it.value,
//                            StandardCharsets.UTF_8.name()
//                        )
//                    }"
//                }
//
//                connection.outputStream.use { it.write(postData.toByteArray()) }
//
//                val responseText = try {
//                    connection.inputStream.bufferedReader().use { it.readText() }
//                } catch (e: Exception) {
//                    println(e)
//                    connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
//                }
//                if(connection.responseCode !in 200..299) {
//                    print(connection.responseMessage)
//                    throw Exception("Query with id: $id failed!")
//                }
//                val jsonResponse = JSONObject(responseText)
//                val resultArray = jsonResponse.getJSONArray("results")
//                resultArray.length()
//            } catch (e: Exception) {
//                println("Thread-$id: Error -> ${e.message}")
//                fail(e.message)
//            }
//        }
//
//        runBlocking {
//            val iterations = listOf(2, 4, 10, 20, 50, 100, 150, 200)
//            val endpoint = "http://localhost:19002/query/service"
//            val folder = Paths.get("results/test_parallelism_results").toFile()
//            if (!folder.exists()) {
//                folder.mkdirs()
//            }
//
//            val csvFile = File(folder, "parallel_query_results.csv")
//            if (!csvFile.exists() || csvFile.readText().isBlank()) {
//                csvFile.writeText("type,numQueries,elapsedTime\n")
//            }
//            for(iteration in iterations){
//                println("Running iteration $iteration")
//
//                // Evaluate Parallel query time
//                val parallelTime = measureTimeMillis {
//
//                    val jobs = (1..iteration).map {
//                            id -> async{query(id, endpoint, "USE Measurements_Dataverse; SELECT * FROM dataset_$id LIMIT 3000")}
//                    }
//                    jobs.awaitAll()
//                }
//
//                // Evaluate sequential query time
//                val sequentialTime = measureTimeMillis {
//                    (1 .. iteration).map{
//                        id -> query(id,endpoint,"USE Measurements_Dataverse; SELECT * FROM dataset_$id LIMIT 3000" )
//                    }
//                }
//
//                // Evaluating one single query time
//                val unionQuery = "USE Measurements_Dataverse;\n " + (1 until iteration).joinToString("UNION ALL ") { qId ->
//                    "SELECT * FROM (SELECT * FROM dataset_$qId LIMIT 3000) as q$qId\n "
//                }
//                val singleQueryTime = measureTimeMillis {
//                    query(-1, endpoint, unionQuery)
//                }
//
//                println("Execution of $iteration parallel queries took $parallelTime ms")
//                println("Execution of $iteration sequential queries took $sequentialTime ms")
//                println("Execution $iteration union query took $singleQueryTime ms")
//
//                csvFile.appendText("parallel,$iteration,$parallelTime\n")
//                csvFile.appendText("sequential,$iteration,$sequentialTime\n")
//                csvFile.appendText("union,$iteration,$singleQueryTime\n")
//            }
//        }
//    }
}