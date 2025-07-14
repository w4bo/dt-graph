import io.lettuce.core.RedisClient
import io.lettuce.core.api.sync.RedisCommands
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Point
import org.locationtech.jts.io.geojson.GeoJsonReader
import java.io.*
import java.lang.Thread.sleep
import java.net.HttpURLConnection
import java.net.Socket
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.format.DateTimeFormatter
import kotlin.random.Random
import kotlin.system.measureTimeMillis

class AsterixDataSource(
    val ingestionLatency: Int,
    val asterixIP: String,
    val asterixPort: Int,
    val datafeedIp: String,
    val dataset: String,
    val maxIteration: Int,
    val measurementsPolygon: Geometry? = null,
    val tsId: Long,
    val testId: Int,
    val clusterMachineNumber: Int,
    val dataSourcesNumber:Int,
    seed: Int,
) {
    private lateinit var socket: Socket
    private lateinit var outputStream: OutputStream
    private val random = Random(seed)

    init {
        try {
            socket = Socket(datafeedIp, asterixPort)
            outputStream = socket.getOutputStream()
            print("Just started a socket on $datafeedIp:$asterixPort")
        } catch (e: Exception) {
            println(e)
            e.printStackTrace()
        }
    }

    private fun randomPointInGeometry(geometry: Geometry?): Point {
        val factory = GeometryFactory()

        return if (geometry == null) {
            val lon = random.nextDouble(-180.0, 180.0)
            val lat = random.nextDouble(-90.0, 90.0)
            factory.createPoint(Coordinate(lon, lat))
        } else {
            val envelope = geometry.envelopeInternal
            val x = random.nextDouble(envelope.minX, envelope.maxX)
            val y = random.nextDouble(envelope.minY, envelope.maxY)
            return factory.createPoint(Coordinate(x, y))
            }
        }

    private fun appendRowsToCSV(outputPath: String, rows: List<Map<String, Any>>, isFirstRow: Boolean = false) {
        if (rows.isEmpty()) return  // niente da scrivere

        val file = File(outputPath)
        file.parentFile?.mkdirs()

        BufferedWriter(FileWriter(file, true)).use { writer ->
            val firstRow = rows.first()
            val csvFormat = if (isFirstRow) {
                CSVFormat.DEFAULT.withHeader(*firstRow.keys.toTypedArray())
            } else {
                CSVFormat.DEFAULT
            }

            CSVPrinter(writer, csvFormat).use { csvPrinter ->
                for (row in rows) {
                    csvPrinter.printRecord(row.values)
                }
                csvPrinter.flush()
            }
        }
    }

    private fun generateMeasurement(iteration: Int): String {
        val timestamp = System.currentTimeMillis()
        val instant = Instant.ofEpochMilli(timestamp)
        val formatter = DateTimeFormatter.ISO_INSTANT
        val formattedTimestamp =  formatter.format(instant)
        return """
        {
            "timestamp": $iteration,
            "property": "Measurement",
            "location": "${randomPointInGeometry(measurementsPolygon)}",
            "fromTimestamp": datetime("1970-01-01T00:00:00.000"),
            "toTimestamp": datetime("1970-01-01T00:00:00.000"),
            "value": 12
        }
        """.trimIndent()
    }


    fun pushToAsterix(outputPath:String) {
        val writer = PrintWriter(outputStream, true)
        val measurements = (0..maxIteration).map { generateMeasurement(it) }.withIndex()
        var rows = mutableListOf<Map<String, Any>>()
            for ((iteration, measurement) in measurements) {
                val insertionTimestamp = System.currentTimeMillis()
                val start = System.nanoTime()
                writer.println(measurement)
                val insertTime = System.nanoTime() - start
                val newRow = mapOf(
                    "testId" to testId,
                    "clusterMachineNumber" to clusterMachineNumber,
                    "dataSourcesNumber" to dataSourcesNumber,
                    "totalInsertions" to maxIteration,
                    "ingestionLatency" to ingestionLatency,
                    "dataSourceId" to tsId,
                    "insertionId" to "$iteration",
                    "operation" to "INSERT",
                    "insertionTimestamp" to insertionTimestamp,
                    "elapsedTime" to insertTime
                )
                rows.add(newRow)
                if(iteration != 0 && iteration % 1000 == 0){
                    appendRowsToCSV(outputPath, rows, isFirstRow = !File(outputPath).exists())
                    rows = mutableListOf<Map<String, Any>>()
                }
                sleep(ingestionLatency.toLong())
            }
            writer.close()


            val connection = URL("http://$asterixIP:19002/query/service").openConnection() as HttpURLConnection
            connection.requestMethod = "POST"
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
            connection.doOutput = true

            val params = mapOf(
                "statement" to "USE Measurements_Dataverse; SELECT AVG(`value`) FROM $dataset",
                "pretty" to "true",
                "mode" to "immediate",
                "dataverse" to "Measurements_Dataverse"
            )

            val postData = params.entries.joinToString("&") {
                "${URLEncoder.encode(it.key, StandardCharsets.UTF_8.name())}=${URLEncoder.encode(it.value, StandardCharsets.UTF_8.name())}"
            }
            val insertionTimestamp = System.currentTimeMillis()
            val selectResult = measureTimeMillis {
                connection.outputStream.use { it.write(postData.toByteArray()) }

                val statusCode = connection.responseCode
                print("Status code: $statusCode")
                when {
                    statusCode in 200..299 -> {
                        print("\nSelect complete")
                    }
                    else -> throw UnsupportedOperationException()
                }
        }

        print("\nSelection time $selectResult")

        val newRow = mapOf(
            "testId" to testId,
            "clusterMachineNumber" to clusterMachineNumber,
            "dataSourcesNumber" to dataSourcesNumber,
            "totalInsertions" to maxIteration,
            "ingestionLatency" to ingestionLatency,
            "dataSourceId" to tsId,
            "insertionId" to "-1",
            "operation" to "READ",
            "insertionTimestamp" to insertionTimestamp,
            "elapsedTime" to selectResult
        )

        appendRowsToCSV(outputPath, listOf(newRow), isFirstRow = false)

    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val randomSeed = System.getenv("SEED")?.toIntOrNull() ?: 42
            val random = Random(randomSeed)
            val ingestionLatency = System.getenv("INGESTION_LATENCY")?.toIntOrNull() ?: 0
            val dataset : String = System.getenv("DATASET") ?: "OpenMeasurements"
            val asterixIP = System.getenv("ASTERIX_IP") ?: "127.0.0.1"
            val datafeedIP = System.getenv("DATAFEED_IP") ?: "127.0.0.1"
            val asterixPort = System.getenv("ASTERIX_PORT")?.toIntOrNull() ?: 10001
            val maxIteration = System.getenv("MAX_ITERATION")?.toIntOrNull() ?: 500000
            val tsId = System.getenv("TS_ID")?.toLongOrNull() ?: random.nextLong(0, Long.MAX_VALUE)
            val testId = System.getenv("TEST_ID")?.toIntOrNull() ?: 0
            val dataSourcesNumber = System.getenv("DATASOURCES_NUMBER")?.toIntOrNull() ?: 1
            val asterixClusterMachines = System.getenv("ASTERIX_MACHINES_COUNT")?.toIntOrNull() ?: 1
            val measurementsPolygon: Geometry? = System.getenv("MEASUREMENTS_POLYGON")?.let { parseGeoJson(it) }

            println(
                """
                Starting an AsterixDB data source...
                Parameters:
                - INGESTION_LATENCY: $ingestionLatency
                - ASTERIX_IP: $asterixIP
                - DATAFEED_IP: $datafeedIP
                - ASTERIX_PORT: $asterixPort
                - MAX_ITERATION: $maxIteration
                - TIMESERIES_ID: $tsId
                - TEST ID : $testId
                - MEASUREMENTS_POLYGON: ${measurementsPolygon?.toText() ?: "NULL"}
                """.trimIndent()
            )

            val dataSource = AsterixDataSource(ingestionLatency,asterixIP, asterixPort, datafeedIP, dataset, maxIteration, measurementsPolygon, tsId, testId,asterixClusterMachines,  dataSourcesNumber, randomSeed)
            val outputPath = "/asterix_statistics/${dataSourcesNumber}dataSources_maxIterations${maxIteration}_${asterixClusterMachines}cluster/tsId${tsId}_date${System.currentTimeMillis()}.csv"

            try {
                dataSource.pushToAsterix(outputPath)
            }

            catch (e: Exception) {
                println("Something went wrong while pushing to AsterixDB")
                val redisHost: String = System.getenv("REDIS_HOST") ?: "localhost"
                val failedKey = System.getenv("BARRIER_FAILED_NAME") ?: "RUN_FAILED"

                val redisClient = RedisClient.create("redis://$redisHost:6379")
                val connection = redisClient.connect()
                val syncCommands: RedisCommands<String, String> = connection.sync()

                syncCommands.incr(failedKey)
                connection.close()
                redisClient.shutdown()
            }
        }

        fun parseGeoJson(geojson: String): Geometry? {
            return try {
                val reader = GeoJsonReader(GeometryFactory())
                val geometry = reader.read(geojson)
                if (geometry is org.locationtech.jts.geom.Polygon) geometry else null
            } catch (e: Exception) {
                println("Invalid GeoJSON Polygon: $geojson")
                null
            }
        }
    }
}
