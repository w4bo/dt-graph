package it.unibo.graph.asterixdb;

import it.unibo.graph.utils.*
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


    fun getDataset() : String = dataset
    fun getDataFeedPort(): Int = dataFeedPort

    init{
        if(!get){
            //If it's not, just try to use the old port
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
            CREATE DATASET $dataset($dataType)IF NOT EXISTS primary key timestamp;
            CREATE INDEX measurement_location_$tsId on $dataset(location) type rtree;
        """.trimIndent()
        val hostIP = if (dataFeedIp != "asterixdb") dataFeedIp else "localhost"
        var dataFeedSetupQuery = """
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
                    CONNECT FEED MeasurementsFeed_$tsId TO DATASET $dataset;
                    START FEED $feedName;
                """.trimIndent()
        val datasetExists = checkDatasetExists(dataverse, dataset)
        var datasetSetup = false

        // IF dataset does not exist, create it
        if(!datasetExists){
            datasetSetup = queryAsterixDB(datasetSetupQuery)
        }

        //If dataset already existed or I've successfully created it
        if(datasetSetup || datasetExists){
            var socketConnect: Boolean
            var datafeedSetup = queryAsterixDB(dataFeedSetupQuery)

            socketConnect = tryDataFeedConnection(dataFeedIp, newDataFeedPort)
            // Until I've succesffully created a DataFeed and I can actually connect to it
            while(!datafeedSetup || !socketConnect){
                // TODO: cap the number of retries and fail if it doesn't work
                // TODO: Update this new port generation, should be more deterministic
                newDataFeedPort = randomDataFeedPort()
                datafeedSetup = setupDataFeed(hostIP, newDataFeedPort, dataverse, feedName, dataset, dataType)

                if(datafeedSetup){
                    socketConnect = tryDataFeedConnection(dataFeedIp, newDataFeedPort)
                }
            }
            dataFeedPort = newDataFeedPort
            return true
        }else{
            return false
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

    private fun tryDataFeedConnection(dataFeedIp: String, dataFeedPort: Int): Boolean{
        try {
            val testSocket = Socket(dataFeedIp, dataFeedPort)
            testSocket.close()
            return true
        }catch(e: Exception){
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

    private fun queryAsterixDB(query: String, checkResults: Boolean = false): Boolean {
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

        val responseText = try {
            connection.inputStream.bufferedReader().use { it.readText() }
        } catch (e: Exception) {
            connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
            throw UnsupportedOperationException(e)
        }

        if(connection.responseCode in 200..299) {
            // If I just want the request to return successfully
            if(!checkResults){
                return true
            }else{
                // Check if it returned something
                val isEmpty = Regex("\"results\"\\s*:\\s*\\[\\s*]").containsMatchIn(responseText)
                return !isEmpty
            }
        }
        else {
            println(responseText)
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
        } catch (e: Exception) {
            println(e)
            connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
            throw Exception()
        }

        return when {
            statusCode in 200..299 -> {
                val cleanedQuery = queryStatement.substringAfter(";").trimStart()
                if (cleanedQuery.startsWith("SELECT", ignoreCase = true)) {
                    try {
                        val jsonResponse = JSONObject(responseText)
                        val resultArray = jsonResponse.getJSONArray("results")
                        if(!isGroupBy){
                            return AsterixDBResult.SelectResult(resultArray)
                        }else{
                            return AsterixDBResult.GroupByResult(resultArray)
                        }
                    } catch (e: Exception) {
                        println(e)
                        println("Error parsing JSON response: ${e.message}")
                        throw Exception("Error parsing JSON response: ${e.message}")
                    }
                } else {
                    AsterixDBResult.InsertResult
                }
            }
            else -> {
                println("Query failed with status code $statusCode")
                throw Exception("Query failed with status code $statusCode")
            }
        }
    }

    fun deleteTs(deleteDataset: Boolean = true){
        val sqlStatement : String
        if(deleteDataset) {
            sqlStatement = """
                USE $dataverse;
                STOP FEED $feedName;
                DROP FEED $feedName;
                DROP dataset $dataset;
            """.trimIndent()
        }else{
            sqlStatement = """ 
                USE $dataverse;
                STOP FEED $feedName;
                DROP FEED $feedName;
                """.trimIndent()
        }
        queryAsterixDB(sqlStatement)
    }
}
