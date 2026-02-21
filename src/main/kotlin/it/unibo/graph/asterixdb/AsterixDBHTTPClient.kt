package it.unibo.graph.asterixdb

import it.unibo.graph.utils.*
import org.json.JSONArray
import org.json.JSONObject
import java.net.*
import java.nio.charset.StandardCharsets
import kotlin.random.Random

class AsterixDBHTTPClient(
        private val clusterControllerHost: String,
        dataFeedIp: String,
        private var dataFeedPort: Int,
        private val dataverse: String,
        datatype: String,
        private val tsId: Long,
        seed: Long,
        get : Boolean = false
) {
    private val feedName: String = "$DATAFEED_PREFIX$tsId"
    private val dataset: String = "$DATASET_PREFIX$tsId"
    fun getDataset(): String = dataset
    fun getDataFeedPort(): Int = dataFeedPort

    init {
        if (!get) {
            // If it's not, just try to use the old port
            initializeTS(dataFeedIp, dataFeedPort, datatype)
            Random(seed)
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

    fun isDatasetEmpty(dataverse : String, dataset : String) : Boolean {
        val checkDatasetExistence = """
            USE $dataverse;
            SELECT *
            FROM $dataset
            LIMIT 1;
        """.trimIndent()
        return !queryAsterixDB(checkDatasetExistence, checkResults = true)
    }

    private fun initializeTS(dataFeedIp: String, feedPort: Int, dataType: String) : Boolean{
        var newDataFeedPort = feedPort

        val datasetSetupQuery = """
            USE $dataverse;
            CREATE DATASET $dataset($dataType) IF NOT EXISTS primary key id;
            CREATE INDEX event_${LOCATION}_$tsId on $dataset($LOCATION) type rtree;
        """.trimIndent()
        val hostIP = if (dataFeedIp != "asterixdb") dataFeedIp else "localhost"
        val datasetExists = checkDatasetExists(dataverse, dataset)
        var datasetSetup = false

        // IF dataset does not exist, create it
        if (!datasetExists) {
            datasetSetup = queryAsterixDB(datasetSetupQuery)
        }
        try {
            // If dataset already existed or I've successfully created it
            if (datasetSetup || datasetExists) {
                var socketConnect: Boolean
                queryAsterixDB("USE $dataverse; STOP FEED $feedName;", blockOnError = false) // The feed could not exist yet
                val dataFeedSetupQuery = """
                    USE $dataverse;
                    DROP FEED $feedName IF EXISTS;
                    CREATE FEED $feedName WITH {
                        "adapter-name": "socket_adapter",
                        "sockets": "${hostIP}:$newDataFeedPort",
                        "address-type": "IP",
                        "type-name": "$dataType",
                        "policy": "Spill",
                        "format": "adm"
                    };
                    CONNECT FEED EventsFeed_$tsId TO DATASET $dataset;
                    START FEED $feedName;
                """.trimIndent()

                // Try to set up a DataFeed
                var datafeedSetup = queryAsterixDB(dataFeedSetupQuery)
                // Try to connect to it
                socketConnect = tryDataFeedConnection(dataFeedIp, newDataFeedPort)
                // Until I've successfully created a DataFeed and I can actually connect to it
                while (!datafeedSetup || !socketConnect) {
                    // TODO: Cap the number of retries and fail if it doesn't work
                    // TODO: Update this new port generation, should be more deterministic
                    newDataFeedPort = randomDataFeedPort()
                    datafeedSetup = setupDataFeed(hostIP, newDataFeedPort, dataverse, feedName, dataset, dataType)
                    if (datafeedSetup) {
                        socketConnect = tryDataFeedConnection(dataFeedIp, newDataFeedPort)
                    }
                }
                dataFeedPort = newDataFeedPort
                return true
            } else {
                return false
            }
        } catch (e: Exception) {
            e.printStackTrace()
            newDataFeedPort = randomDataFeedPort()
            return initializeTS(dataFeedIp, newDataFeedPort, dataType)
        }
    }

    private fun setupDataFeed(dataFeedIp: String, dataFeedPort: Int, dataverse: String, feedName: String, dataset: String, dataType: String) : Boolean{
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
            val testSocket = Socket(dataFeedIp, dataFeedPort)
            testSocket.close()
            return true
        } catch (e: Exception) {
            e.printStackTrace()
            println("Failed to open a socket, stopping datafeed and will try to open a new one")
            deleteTs(deleteDataset = false)
            return false
        }
    }

    fun randomDataFeedPort(): Int{
        return generateSequence { (FIRSTFEEDPORT + tsId.toInt()..LASTFEEDPORT).random() }
            .distinct()
            .firstOrNull() ?: throw IllegalStateException("No available port found")
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

    fun deleteTs(deleteDataset: Boolean = true) {
        var sql = """
            USE $dataverse;
            STOP FEED $feedName;
            DROP FEED $feedName;
        """.trimIndent()
         if (deleteDataset) {
            sql += "\nDROP dataset $dataset;"
        }
        queryAsterixDB(sql)
    }

}
