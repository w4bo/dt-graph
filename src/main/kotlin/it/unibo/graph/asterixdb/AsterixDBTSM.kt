package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
import it.unibo.graph.utils.EDGES
import it.unibo.graph.utils.ID
import it.unibo.graph.utils.KEY
import it.unibo.graph.utils.LABEL
import it.unibo.graph.utils.LOCATION
import it.unibo.graph.utils.PROPERTIES
import it.unibo.graph.utils.TYPE
import it.unibo.graph.utils.VALUE
import it.unibo.graph.utils.loadProps
import java.net.HttpURLConnection
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

val props = loadProps()

class AsterixDBTSM private constructor(
    override val g: Graph,
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
                    `$KEY`: string,
                    `$VALUE`: PropertyValue,
                    `$TYPE`: int
                };

                CREATE TYPE Edge AS CLOSED {
                    $LABEL: string,
                    fromN: bigint,
                    toN: bigint,
                    $PROPERTIES: [Property]?
                };

                CREATE TYPE Event AS OPEN {
                    $ID: bigint,
                    $LABEL: string,
                    $LOCATION: geometry?,
                    $EDGES: [Edge]?,
                    $PROPERTIES: [Property]?
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
}
