package it.unibo.graph.asterixdb;

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.utils.*
import org.json.JSONObject
import java.net.*
import java.nio.charset.StandardCharsets
import kotlin.random.Random

class AsterixDBHTTPClient(
        private val clusterControllerHost: String,
        dataFeedIp: String,
        private val busyPorts: MutableSet<Int>,
        private val dataverse: String,
        datatype: String,
        private val tsId: Long,
        seed: Int = SEED,

) {
    private var dataFeedPort : Int
    private val feedName: String = "$DATAFEED_PREFIX$tsId"
    private val dataset: String


    fun getDataset() : String = dataset
    fun getDataFeedPort(): Int = dataFeedPort

    init{

        Random(seed)
        dataset = "$DATASET_PREFIX$tsId"
        dataFeedPort = FIRSTFEEDPORT + tsId.toInt()
        if(dataset=="dataset_601"){
            println("HELO")
        }
        val datasetSetupQuery = """
            USE $dataverse;
            CREATE DATASET $dataset($datatype)IF NOT EXISTS primary key timestamp;
            CREATE INDEX measurement_location_$tsId on $dataset(location) type rtree;
        """.trimIndent()
        var dataFeedSetupQuery = """
                    USE $dataverse;
                    DROP FEED $feedName IF EXISTS;
                    CREATE FEED $feedName WITH {
                      "adapter-name": "socket_adapter",
                      "sockets": "$dataFeedIp:$dataFeedPort",
                      "address-type": "IP",
                      "type-name": "$datatype",
                      "policy": "Spill",
                      "format": "adm"
                    };
                    CONNECT FEED MeasurementsFeed_$tsId TO DATASET $dataset;
                    START FEED $feedName;
                """.trimIndent()
        val datasetSetup = queryAsterixDB(datasetSetupQuery)
        if(datasetSetup){
            var socketConnect: Boolean
            var datafeedSetup = queryAsterixDB(dataFeedSetupQuery)

            try {
                val testSocket = Socket(dataFeedIp, dataFeedPort)
                socketConnect = true
                testSocket.close()
            }catch(e: Exception){
                println("Failed to open a socket, stopping datafeed and will try to open a new one")
                deleteTs(deleteDataset = false)
                socketConnect = false
            }

            while(!datafeedSetup || !socketConnect){
                busyPorts.add(dataFeedPort)
                dataFeedPort = generateSequence { (FIRSTFEEDPORT + tsId.toInt()..MAX_TS).random() }
                    .distinct()
                    .filterNot { it in busyPorts }
                    .firstOrNull() ?: throw IllegalStateException("No available port found")
                dataFeedSetupQuery = """
                    USE $dataverse;
                    DROP FEED $feedName IF EXISTS;
                    CREATE FEED $feedName WITH {
                      "adapter-name": "socket_adapter",
                      "sockets": "$dataFeedIp:$dataFeedPort",
                      "address-type": "IP",
                      "type-name": "$datatype",
                      "policy": "Spill",
                      "format": "adm"
                    };
                    CONNECT FEED $feedName TO DATASET $dataset;
                    START FEED $feedName;
                """.trimIndent()
                datafeedSetup = queryAsterixDB(dataFeedSetupQuery)

                if(datasetSetup){
                    try {
                        val testSocket = Socket(dataFeedIp, dataFeedPort)
                        socketConnect = true
                        testSocket.close()
                    }catch(e: Exception){
                        println("Failed to open a socket, stopping datafeed and will try to open a new one")
                        deleteTs(deleteDataset = false)
                        socketConnect = false
                    }
                }
            }
        }
    }

    private fun queryAsterixDB(query: String): Boolean {
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
            return true
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
                        AsterixDBResult.ErrorResult
                    }
                } else {
                    AsterixDBResult.InsertResult
                }
            }
            else -> {
                println("Query failed with status code $statusCode")
                AsterixDBResult.ErrorResult
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
