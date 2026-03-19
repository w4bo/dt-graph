package it.unibo.graph.asterixdb

import it.unibo.graph.utils.*
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.net.Socket

class AsterixDBHTTPClient(
    private val clusterControllerHost: String,
    val dataFeedIp: String,
    var dataFeedPort: Int,
    private val dataverse: String,
    val dataType: String,
    private val tsId: Long = 0L,
    val dataset: String = "$DATASET_PREFIX$tsId",
    val feedName: String = "$DATAFEED_PREFIX$tsId"
) {
    private val logger = LoggerFactory.getLogger(AsterixDBHTTPClient::class.java)

    fun stopFeed() = queryAsterixDB("USE $dataverse; STOP FEED $feedName;", blockOnError = false) // The feed could not exist yet

    fun createDataset(dataType: String): Boolean {
        return queryAsterixDB("USE $dataverse; CREATE DATASET $dataset($dataType) IF NOT EXISTS primary key $TSID, $ID;")
    }

    fun createSpatialIndex(): Boolean {
        return queryAsterixDB("USE $dataverse; CREATE INDEX ${LOCATION}_$tsId on $dataset($LOCATION) type rtree;")
    }

    fun initializeTS() : Boolean {
        val datasetExists = createDataset(dataType)
        var failures = 0
        if (datasetExists) { // If dataset already existed or I have successfully created it
            do { // Until I have successfully created a DataFeed and I can actually connect to it
                var datafeedSetup = false
                var socketConnect = false
                try {
                    stopFeed()
                    datafeedSetup = setupDataFeed(/*hostIP*/) // val hostIP = if (dataFeedIp != "asterixdb") dataFeedIp else "localhost"
                    if (!datafeedSetup) {
                        val msg = "Cannot setup feed: $dataFeedPort"
                        logger.warn(msg)
                        failures += 1
                        throw IllegalArgumentException(msg)
                    }
                    socketConnect = tryDataFeedConnection(dataFeedIp, dataFeedPort) // Try to connect to it
                    if (!socketConnect) {
                        val msg = "Cannot connect to feed: $dataFeedPort"
                        logger.warn(msg)
                        failures += 1
                        throw IllegalArgumentException(msg)
                    }
                    if (failures > 0) logger.warn("Failures: $failures")
                    return true
                } catch (e: Exception) {
                    e.printStackTrace()
                    dataFeedPort = incPort()
                }
            } while (!datafeedSetup && !socketConnect)
        }
        return false
    }

    fun dropFeed(): Boolean {
        return queryAsterixDB("USE $dataverse; DROP FEED $feedName IF EXISTS;")
    }

    fun setupDataFeed(): Boolean {
        val dataFeedSetupQuery = """
            USE $dataverse;
            DROP FEED $feedName IF EXISTS;
            CREATE FEED $feedName WITH {
                "adapter-name": "socket_adapter",
                "sockets": "$dataFeedIp:$dataFeedPort",
                "address-type": "IP",
                "type-name": "$dataType",
                "format": "adm"
            };
            CONNECT FEED $feedName TO DATASET $dataset;
            START FEED $feedName;
        """.trimIndent()
        return queryAsterixDB(dataFeedSetupQuery)
    }

    private fun tryDataFeedConnection(dataFeedIp: String, dataFeedPort: Int): Boolean {
        try {
            Socket(dataFeedIp, dataFeedPort).close()
            return true
        } catch (_: Exception) {
            return false
        }
    }

    private fun queryAsterixDB(sql: String, checkResults: Boolean = false, blockOnError: Boolean = true): Boolean {
        val connection = query(sql, clusterControllerHost, dataverse)
        val responseCode = connection.responseCode
        val responseText = try {
            connection.inputStream.bufferedReader().use { it.readText() }
        } catch (_: Exception) {
            val errMsg = connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
            if (blockOnError) throw UnsupportedOperationException(errMsg)
            errMsg
        }

        if (responseCode in 200..299) {
            if (!checkResults) { // If I just want the request to return successfully
                return true
            } else {
                // Check if it returned something
                val isEmpty = Regex("\"results\"\\s*:\\s*\\[\\s*]").containsMatchIn(responseText)
                return !isEmpty
            }
        } else {
            return false
        }
    }

    fun updateTs(upsertStatement: String) {
        queryAsterixDB(upsertStatement)
    }

    fun selectFromAsterixDB(sql: String, isGroupBy: Boolean = false): AsterixDBResult {
        val connection = query(sql, clusterControllerHost, dataverse)
        val responseCode = connection.responseCode
        val responseText = try {
            connection.inputStream.bufferedReader().use { it.readText() }
        } catch (_: Exception) {
            val errMsg = connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
            if (!errMsg.contains("ASX1077")) { // The dataset could not exist: e.g. when a GS node does not have an assigned ts in TS yet
                throw UnsupportedOperationException("SQL:\n ${sql}\nError:\n$errMsg")
            } else {
                errMsg
            }
        }

        return when (responseCode) {
            in 200..299 -> {
                val cleanedQuery = sql.substringAfter(";").trimStart()
                if (cleanedQuery.startsWith("SELECT", ignoreCase = true)) {
                    try {
                        val jsonResponse = JSONObject(responseText)
                        val resultArray = jsonResponse.getJSONArray("results")
                        if (!isGroupBy) {
                            AsterixDBResult.SelectResult(resultArray)
                        } else {
                            AsterixDBResult.GroupByResult(resultArray)
                        }
                    } catch (e: Exception) {
                        throw Exception("Error parsing JSON response: ${e.message}")
                    }
                } else {
                    AsterixDBResult.InsertResult
                }
            }

            400 ->
                if (!isGroupBy) {
                    AsterixDBResult.SelectResult(JSONArray())
                } else {
                    AsterixDBResult.GroupByResult(JSONArray())
                }

            else -> throw UnsupportedOperationException("Query failed with status code $responseCode: $responseText")
        }
    }

    fun createTSMEnvironment(): Boolean {
        val sql =
            """
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
                    $TSID: bigint,
                    $ID: bigint,
                    $LABEL: int,
                    $LOCATION: geometry?,
                    $EDGES: [Edge]?
                };
            """.trimIndent()
        return queryAsterixDB(sql)
    }
}
