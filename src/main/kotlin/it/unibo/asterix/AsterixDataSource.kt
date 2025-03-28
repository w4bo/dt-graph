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
import kotlin.system.measureTimeMillis

class AsterixDataSource(
    val ingestionLatency: Int,
    asterixIP: String,
    asterixPort: Int,
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
        file.parentFile?.mkdirs() // Crea la cartella se non esiste

        // Usa `FileWriter` con `append = true` per aggiungere senza sovrascrivere
        BufferedWriter(FileWriter(file, true)).use { writer ->
            val csvFormat = if (isFirstRow) {
                CSVFormat.DEFAULT.withHeader(*row.keys.toTypedArray()) // Scrive l'header solo se il file è nuovo
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
        print("Insertion time: $result")
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            val ingestionLatency = System.getenv("INGESTION_LATENCY")?.toIntOrNull() ?: 0
            val asterixIP = System.getenv("ASTERIX_IP") ?: "127.0.0.1"
            val asterixPort = System.getenv("ASTERIX_PORT")?.toIntOrNull() ?: 10001
            val maxIteration = System.getenv("MAX_ITERATION")?.toIntOrNull() ?: 10000
            val tsId = System.getenv("TS_ID")?.toLongOrNull() ?: 15L
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
