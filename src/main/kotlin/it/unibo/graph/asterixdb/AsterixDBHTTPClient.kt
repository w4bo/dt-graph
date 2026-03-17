package it.unibo.graph.asterixdb

import it.unibo.graph.utils.DATAFEED_PREFIX
import it.unibo.graph.utils.DATASET_PREFIX
import it.unibo.graph.utils.LOCATION
import it.unibo.graph.utils.incPort
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.net.*
import java.nio.charset.StandardCharsets
import kotlin.concurrent.atomics.ExperimentalAtomicApi

class AsterixDBHTTPClient(
        private val clusterControllerHost: String,
        dataFeedIp: String,
        private var dataFeedPort: Int,
        private val dataverse: String,
        datatype: String,
        private val tsId: Long,
        get : Boolean = false,
        createSpatialIndex: Boolean,
) {
    private val logger = LoggerFactory.getLogger(AsterixDBHTTPClient::class.java)
    private val feedName: String = "$DATAFEED_PREFIX$tsId"
    private val dataset: String = "$DATASET_PREFIX$tsId"
    fun getDataset(): String = dataset
    fun getDataFeedPort(): Int = dataFeedPort

    init {
        if (!get) {
            initializeTS(dataFeedIp, dataFeedPort, datatype)  // If it's not, just try to use the old port
        }
    }

    fun checkDatasetExists(dataverse : String, dataset : String) : Boolean {
        val checkDatasetExistence = """
            SELECT VALUE ds
            FROM Metadata.`Dataset` ds
            WHERE ds.DataverseName = "$dataverse" AND ds.DatasetName = "$dataset";      
        """.trimIndent()
        return queryAsterixDB(checkDatasetExistence, checkResults = true)
    }

    fun stopFeed() = queryAsterixDB("USE $dataverse; STOP FEED $feedName;", blockOnError = false) // The feed could not exist yet

    fun createDataset(dataType: String): Boolean {
        val datasetSetupQuery = """
                USE $dataverse;
                CREATE DATASET $dataset($dataType) IF NOT EXISTS primary key id;
                CREATE INDEX ${LOCATION}_$tsId on $dataset($LOCATION) type rtree;
            """.trimIndent()
        return queryAsterixDB(datasetSetupQuery)
    }

    @OptIn(ExperimentalAtomicApi::class)
    private fun initializeTS(dataFeedIp: String, feedPort: Int, dataType: String) : Boolean {
        dataFeedPort = feedPort
        val hostIP = if (dataFeedIp != "asterixdb") dataFeedIp else "localhost"

        var datasetExists = checkDatasetExists(dataverse, dataset)
        if (!datasetExists) { // If the dataset does not exist, create it
            datasetExists = createDataset(dataType)
        }
        var failures = 0
        if (datasetExists) { // If dataset already existed or I have successfully created it
            do { // Until I have successfully created a DataFeed and I can actually connect to it
                var datafeedSetup = false
                var socketConnect = false
                try {
                    stopFeed()
                    datafeedSetup = setupDataFeed(hostIP, dataFeedPort, dataverse, feedName, dataset, dataType)
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

    private fun setupDataFeed(dataFeedIp: String, dataFeedPort: Int, dataverse: String, feedName: String, dataset: String, dataType: String): Boolean {
        val dataFeedSetupQuery = """
            USE $dataverse;
            DROP FEED $feedName IF EXISTS;
            CREATE FEED $feedName WITH {
                "adapter-name": "socket_adapter",
                "sockets": "$dataFeedIp:$dataFeedPort",
                "address-type": "IP",
                "type-name": "$dataType",
                "policy": "Spill",
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

    private fun queryAsterixDB(query: String, checkResults: Boolean = false, blockOnError: Boolean = true): Boolean {
        val uri = URI(clusterControllerHost)
        val connection = uri.toURL().openConnection() as HttpURLConnection
        connection.requestMethod = "POST"
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
        connection.doOutput = true

        val params = mapOf(
            "statement" to query,
            "pretty" to "true",
            "mode" to "immediate",
            "dataverse" to dataverse
        )

        val postData = params.entries.joinToString("&") {
            "${URLEncoder.encode(it.key, StandardCharsets.UTF_8.name())}=${URLEncoder.encode(it.value, StandardCharsets.UTF_8.name())}"
        }

        connection.outputStream.use { it.write(postData.toByteArray()) }
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

    fun selectFromAsterixDB(queryStatement: String, isGroupBy: Boolean = false): AsterixDBResult {
        val connection = URL(clusterControllerHost).openConnection() as HttpURLConnection
        connection.requestMethod = "POST"
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
        connection.doOutput = true
        val params = mapOf(
            "statement" to queryStatement,
            "pretty" to "true",
            "mode" to "immediate",
            "dataverse" to dataverse
        )
        val postData = params.entries.joinToString("&") {
            "${URLEncoder.encode(it.key, StandardCharsets.UTF_8.name())}=${URLEncoder.encode(it.value, StandardCharsets.UTF_8.name())}"
        }
        connection.outputStream.use { it.write(postData.toByteArray()) }
        val statusCode = connection.responseCode
        val responseText = try {
            connection.inputStream.bufferedReader().use { it.readText() }
        } catch (_: Exception) {
            val errMsg = connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
            if (!errMsg.contains("ASX1077")) { // The dataset could not exist: e.g. when a GS node does not have an assigned ts in TS yet
                throw UnsupportedOperationException(errMsg)
            } else {
                errMsg
            }
        }

        return when (statusCode) {
            in 200..299 -> {
                val cleanedQuery = queryStatement.substringAfter(";").trimStart()
                if (cleanedQuery.startsWith("SELECT", ignoreCase = true)) {
                    return try {
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

            else -> throw UnsupportedOperationException("Query failed with status code $statusCode: $responseText")
        }
    }
}
