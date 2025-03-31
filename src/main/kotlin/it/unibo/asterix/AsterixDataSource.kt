import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.TSManager
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Point
import org.locationtech.jts.io.geojson.GeoJsonReader
import java.lang.Thread.sleep
import java.net.Socket
import kotlin.random.Random
import org.jetbrains.kotlinx.dataframe.api.*
import org.jetbrains.kotlinx.dataframe.io.writeCSV
import java.io.*
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import kotlin.system.measureTimeMillis

class AsterixDataSource(
    val ingestionLatency: Int,
    val asterixIP: String,
    val asterixPort: Int,
    val maxIteration: Int,
    val measurementsPolygon: Geometry? = null,
    val tsId: Long,
    val testId: Int,
    val clusterMachineNumber: Int,
    val dataSourcesNumber:Int,
) {
    private lateinit var socket: Socket
    private lateinit var outputStream: OutputStream
    private val seed = 42
    private val random = Random(seed)

    init {
        try {
            socket = Socket(asterixIP, asterixPort) // Usa dbHost invece di "localhost"
            outputStream = socket.getOutputStream()
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
    private fun appendRowToCSV(outputPath: String, row: Map<String, Any>, isFirstRow: Boolean = false) {
        val file = File(outputPath)
        file.parentFile?.mkdirs()

        BufferedWriter(FileWriter(file, true)).use { writer ->
            val csvFormat = if (isFirstRow) {
                CSVFormat.DEFAULT.withHeader(*row.keys.toTypedArray())
            } else {
                CSVFormat.DEFAULT
            }

            CSVPrinter(writer, csvFormat).use { csvPrinter ->
                csvPrinter.printRecord(row.values)
                csvPrinter.flush()
            }
        }
    }

    private fun generateMeasurement(iteration: Int): String {
        return """
        {
            "id": "$tsId|$iteration",
            "timestamp": datetime("1970-01-01T00:00:00.000"),
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
        //val ts1 = AsterixDBTSM.createDefault(MemoryGraph())

        val result = measureTimeMillis {
            for (iteration in 0..<maxIteration) {
                println("Running iteration $iteration")
                val measurement = generateMeasurement(iteration)
                val insertTime = System.currentTimeMillis()
                writer.println(measurement)
                writer.flush()


                // Crea una mappa che rappresenta una nuova riga
                val newRow = mapOf(
                    "testId" to testId,
                    "clusterMachineNumber" to clusterMachineNumber,
                    "dataSourcesNumber" to dataSourcesNumber,
                    "totalInsertions" to maxIteration,
                    "ingestionLatency" to ingestionLatency,
                    "dataSourceId" to tsId,
                    "insertionId" to "$tsId|$iteration",
                    "insertionTimestamp" to insertTime
                )

                appendRowToCSV(outputPath, newRow, isFirstRow = !File(outputPath).exists())
                sleep(ingestionLatency.toLong())
            }
            writer.close()
        }

        val selectResult = measureTimeMillis {
            val connection = URL("http://$asterixIP:19002/query/service").openConnection() as HttpURLConnection
            connection.requestMethod = "POST"
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
            connection.doOutput = true

            val params = mapOf(
                "statement" to "SELECT COUNT(*) FROM OpenMeasurements",
                "pretty" to "true",
                "mode" to "immediate",
                "dataverse" to "Measurements_Dataverse"
            )

            val postData = params.entries.joinToString("&") {
                "${URLEncoder.encode(it.key, StandardCharsets.UTF_8.name())}=${URLEncoder.encode(it.value, StandardCharsets.UTF_8.name())}"
            }

            connection.outputStream.use { it.write(postData.toByteArray()) }

            val statusCode = connection.responseCode
            print("Status code: $statusCode")
            when {
                statusCode in 200..299 -> {
                    print("Select complete")
                }
                else -> throw UnsupportedOperationException()
            }
        }

        print("Insertion time: $result")
        print("Selection time $selectResult")
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            val ingestionLatency = System.getenv("INGESTION_LATENCY")?.toIntOrNull() ?: 0
            val asterixIP = System.getenv("ASTERIX_IP") ?: "127.0.0.1"
            val asterixPort = System.getenv("ASTERIX_PORT")?.toIntOrNull() ?: 10001
            val maxIteration = System.getenv("MAX_ITERATION")?.toIntOrNull() ?: 500000
            val tsId = System.getenv("TS_ID")?.toLongOrNull() ?: 2L
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
                - ASTERIX_PORT: $asterixPort
                - MAX_ITERATION: $maxIteration
                - TIMESERIES ID: $tsId
                - TEST ID : $testId
                - MEASUREMENTS_POLYGON: ${measurementsPolygon?.toText() ?: "NULL"}
                """.trimIndent()
            )

            val dataSource = AsterixDataSource(ingestionLatency,asterixIP,asterixPort,maxIteration, measurementsPolygon, tsId, testId, dataSourcesNumber, asterixClusterMachines )
            val outputPath = "/asterix_statistics/testId${testId}_tsId_${tsId}_maxIterations${maxIteration}.csv"

            dataSource.pushToAsterix(outputPath)
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
