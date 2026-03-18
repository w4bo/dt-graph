package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
import it.unibo.graph.utils.*

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
            if (!createTSMEnvironment()) {
                throw Exception("Something went wrong while creating $dataverse")
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

    fun addAsterixTS(id: Long, createSpatialIndex: Boolean): TS {
        val newTS = AsterixDBTS(g, id + 1, clusterControllerHost, nodeControllersIPs, dataverse, datatype, get = false, createSpatialIndex = createSpatialIndex)
        return newTS
    }

    override fun addTS(id: Long): TS = addAsterixTS(id, false)

    override fun getTS(id: Long): TS {
        return AsterixDBTS(g, id, clusterControllerHost, nodeControllersIPs, dataverse, datatype, get = true, createSpatialIndex = false)
    }

    override fun clear() {
        queryAsterixDB(clusterControllerHost, getDataverseCreationQuery(dataverse))
    }

    // Private utility functions
    private fun queryAsterixDB(host: String, sql: String, checkResults: Boolean = false): Boolean {
        val connection = query(sql, host, dataverse)
        val responseText = try {
            connection.inputStream.bufferedReader().use { it.readText() }
        } catch (e: Exception) {
            e.printStackTrace()
            val errMsg = connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
            throw UnsupportedOperationException(errMsg)
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

                CREATE TYPE Property AS OPEN {
                    `$KEY`: string,
                    `$TYPE`: int
                };

                CREATE TYPE Edge AS CLOSED {
                    $LABEL: int,
                    $FROM_N: bigint,
                    $TO_N: bigint
                };

                CREATE TYPE Event AS OPEN {
                    $ID: bigint,
                    $LABEL: int,
                    $LOCATION: geometry?,
                    $EDGES: [Edge]?
                };
            """.trimIndent()
    }

    companion object {
        fun createDefault(g: Graph, dataverse: String = props["default_dataverse"].toString()): AsterixDBTSM {
            return AsterixDBTSM(
                g,
                System.getenv("ASTERIXDB_CC_HOST") ?: "localhost",
                props["default_cc_port"].toString(),
                dataverse,
                System.getenv("DEFAULT_NC_POOL")?.split(',') ?: listOf("localhost"),
                props["default_datatype"].toString(),
            )
        }
    }
}
