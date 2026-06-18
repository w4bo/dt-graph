package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
<<<<<<< HEAD
import it.unibo.graph.utils.loadProps
import java.net.HttpURLConnection
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
=======
import it.unibo.graph.interfaces.TsMode
import it.unibo.graph.utils.DATASET_PREFIX
import it.unibo.graph.utils.FIRSTFEEDPORT
import it.unibo.graph.utils.LASTFEEDPORT
import it.unibo.graph.utils.loadProps
import kotlin.random.Random
>>>>>>> feat-tssingletable

val props = loadProps()

class AsterixDBTSM private constructor(
    override val g: Graph,
<<<<<<< HEAD
    host: String,
    port: String,
    val dataverse: String,
    val nodeControllersIPs: List<String>,
    val datatype: String,
) : TSManager {
    val clusterControllerHost: String = "http://$host:$port/query/service"

    init {
        if (!checkRestore(dataverse)) {
            val result = createTSMEnvironment()
            if (!result) {
                println("Something went wrong while creating $dataverse environment")
                throw Exception()
            }
        }
    }

    // Check if dataverse already exists
    private fun checkRestore(dataverse: String): Boolean {
        val query = """
            SELECT VALUE dv
            FROM Metadata.`Dataverse` dv
            WHERE dv.DataverseName = "$dataverse";
        """.trimIndent()
        return queryAsterixDB(clusterControllerHost, query, checkResults = true)
    }

    override fun addTS(id: Long): TS {
        val newTS = AsterixDBTS(g, id + 1, clusterControllerHost, nodeControllersIPs, dataverse, datatype)
        return newTS
    }

    override fun getTS(id: Long): TS {
        return AsterixDBTS(g, id, clusterControllerHost, nodeControllersIPs, dataverse, datatype, get = true)
    }

    override fun clear() {
        queryAsterixDB(clusterControllerHost, getDataverseCreationQuery(dataverse))
    }

    // Private utility functions
    private fun queryAsterixDB(host: String, query: String, checkResults: Boolean = false): Boolean {
        val uri = URI(host)
        val connection = uri.toURL().openConnection() as HttpURLConnection
        connection.requestMethod = "POST"
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
        connection.doOutput = true

        val params = mapOf(
            "statement" to query, "pretty" to "true", "mode" to "immediate", "dataverse" to dataverse
        )

        val postData = params.entries.joinToString("&") {
            "${URLEncoder.encode(it.key, StandardCharsets.UTF_8.name())}=${
                URLEncoder.encode(
                    it.value, StandardCharsets.UTF_8.name()
                )
            }"
        }

        connection.outputStream.use { it.write(postData.toByteArray()) }


        val responseText = try {
            connection.inputStream.bufferedReader().use { it.readText() }
        } catch (e: Exception) {
            connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
            throw UnsupportedOperationException(e)
        }

        if (connection.responseCode in 200..299) {
            // If I just want the request to return successfully
            if (!checkResults) {
                return true
            } else {
                // Check if it returned something
                val isEmpty = Regex("\"results\"\\s*:\\s*\\[\\s*]").containsMatchIn(responseText)
                return !isEmpty
            }
        } else {
            println(responseText)
            return false
        }
    }

    private fun createTSMEnvironment(): Boolean {
        return queryAsterixDB(clusterControllerHost, getDataverseCreationQuery(dataverse))
    }

    private fun getDataverseCreationQuery(dataverse: String): String {
        return """
                DROP dataverse $dataverse IF EXISTS;
                CREATE DATAVERSE $dataverse;
                USE $dataverse;

                CREATE TYPE PropertyValue AS OPEN {
                    stringValue: string?,
                    doubleValue: double?,
                    intValue: int?,
                    geometryValue: string?
                };

                CREATE TYPE Property AS CLOSED {
                    sourceId: bigint,
                    sourceType: Boolean,
                    `key`: string,
                    `value`: PropertyValue,
                    `type`: int,
                    fromTimestamp: DATETIME?,
                    toTimestamp: DATETIME?
                };

                CREATE TYPE NodeRelationship AS CLOSED {
                    `type`: string,
                    fromN: bigint,
                    toN: bigint,
                    fromTimestamp: DATETIME?,
                    toTimestamp: DATETIME?,
                    properties: [Property]?
                };

                CREATE TYPE Measurement AS OPEN {
                    timestamp: int,
                    property: STRING,
                    location: geometry?,
                    relationships: [NodeRelationship]?,
                    properties: [Property]?
                };
            """.trimIndent()
    }

    companion object {
        fun createDefault(g: Graph): AsterixDBTSM {
            return AsterixDBTSM(
                g,
                System.getenv("ASTERIXDB_CC_HOST") ?: "localhost",
                props["default_cc_port"].toString(),
                props["default_dataverse"].toString(),
                System.getenv("DEFAULT_NC_POOL")?.split(',') ?: listOf("localhost"),
                props["default_datatype"].toString(),
            )
        }
    }
=======
    val host: String,
    val port: String,
    val dataverse: String,
    val nodeControllersIPs: List<String>,
    val maxConnections: Int? = 100,
    val multiTs: Boolean
) : TSManager {
    val isConcurrent = nodeControllersIPs.size > 1
    val dataset = "${DATASET_PREFIX}0"
    val connection = AsterixDBHTTPClient("http://$host:$port/query/service", getDataFeedIP(), dataverse, dataset, isConcurrent = isConcurrent)
    val connections = mutableListOf(connection)
    val r = Random(0)

    private fun getDataFeedIP(): String {
        val hash = (Math.random() * 100).toInt()
        val index = (hash and 0x7FFFFFFF) % nodeControllersIPs.size
        return nodeControllersIPs[index]
    }

    fun createAndOpenConnection(mode: TsMode): AsterixDBHTTPClient {
        // Determine which connection to use
        val connection = if (isConcurrent) {
            // If we've reached the max number of connections, reuse a random existing connection
            if (connections.size == maxConnections) {
                connections[r.nextInt(0, maxConnections)]
            } else {
                // Otherwise, create a new connection and store it
                val c = AsterixDBHTTPClient("http://$host:$port/query/service", getDataFeedIP(), dataverse, dataset, isConcurrent = isConcurrent)
                connections.add(c)
                c
            }
        } else {
            // In non-concurrent mode, reuse the single existing connection
            connection
        }
        // If the mode is WRITE, ensure the data feed connection is opened
        if (mode === TsMode.WRITE) {
            connection.openDataFeedConnection()
        }
        // Return the selected or newly created connection
        return connection
    }

    override fun addTS(id: Long): TS {
        return AsterixDBTS(g, id + 1,  dataverse, dataset, createAndOpenConnection(TsMode.WRITE))
    }

    override fun getTS(id: Long, mode: TsMode): TS {
        return AsterixDBTS(g, id, dataverse, dataset, createAndOpenConnection(mode))
    }

    override fun clear() {
        close()
        connection.createTSMEnvironment()
        connection.createDataset(multiTs)
    }

    companion object {
        fun createDefault(
            g: Graph,
            dataverse: String = props["default_dataverse"].toString(),
            host: String = System.getenv("ASTERIXDB_CC_HOST") ?: "localhost",
            port: String = props["default_cc_port"].toString(),
            controllerIps: List<String> = System.getenv("DEFAULT_NC_POOL")?.split(',') ?: listOf("localhost"),
            maxConnections: Int? = (LASTFEEDPORT - FIRSTFEEDPORT) / 2,
            multiTs: Boolean = false
        ): AsterixDBTSM {
            return AsterixDBTSM(g, host, port, dataverse, controllerIps, maxConnections, multiTs)
        }
    }

    override fun close() {
        connections.forEach { it.closeDataFeedConnection(true) }
    }
>>>>>>> feat-tssingletable
}
