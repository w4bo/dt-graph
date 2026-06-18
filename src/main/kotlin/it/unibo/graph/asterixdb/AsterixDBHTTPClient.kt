package it.unibo.graph.asterixdb

<<<<<<< HEAD
import it.unibo.graph.utils.DATAFEED_PREFIX
import it.unibo.graph.utils.DATASET_PREFIX
import it.unibo.graph.utils.FIRSTFEEDPORT
import it.unibo.graph.utils.LASTFEEDPORT
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
            CREATE DATASET $dataset($dataType)IF NOT EXISTS primary key timestamp;
            CREATE INDEX measurement_location_$tsId on $dataset(location) type rtree;
        """.trimIndent()
        val hostIP = if (dataFeedIp != "asterixdb") dataFeedIp else "localhost"
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
                    CONNECT FEED MeasurementsFeed_$tsId TO DATASET $dataset;
                    START FEED $feedName;
                """.trimIndent()
        val datasetExists = checkDatasetExists(dataverse, dataset)
        var datasetSetup = false

        // IF dataset does not exist, create it
        if (!datasetExists) {
            datasetSetup = queryAsterixDB(datasetSetupQuery)
        }
        try{
            //If dataset already existed or I've successfully created it
            if(datasetSetup || datasetExists){
                var socketConnect: Boolean
                // Try to set up a DataFeed
                var datafeedSetup = queryAsterixDB(dataFeedSetupQuery)
                // Try to connect to it
                socketConnect = tryDataFeedConnection(dataFeedIp, newDataFeedPort)
                // Until I've successfully created a DataFeed and I can actually connect to it
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
        } catch(_: Exception){
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

    private fun tryDataFeedConnection(dataFeedIp: String, dataFeedPort: Int): Boolean{
        try {
            val testSocket = Socket(dataFeedIp, dataFeedPort)
            testSocket.close()
            return true
        }catch(e: Exception){
            println("Failed to open a socket, stopping datafeed and will try to open a new one")
            deleteTs(deleteDataset = false)
=======
import it.unibo.graph.utils.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.io.Writer
import java.net.Socket
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock


class AsterixDBHTTPClient(
    private val clusterControllerHost: String,
    val dataFeedIp: String,
    private val dataverse: String,
    private val dataset: String,
    private var dataFeedPort: Int = incPort(),
    private val isConcurrent: Boolean
) {
    companion object {
        val id: AtomicInteger = AtomicInteger(0)
    }
    private val id = Companion.id.incrementAndGet()
    private val dataType: String = EVENT
    private val feedName: String = "$DATAFEED_PREFIX${id}"
    private val logger = LoggerFactory.getLogger(AsterixDBHTTPClient::class.java)
    private lateinit var socket: Socket
    private lateinit var outputStream: OutputStream
    private lateinit var writer: Writer
    private var isFeedConnectionOpen: Boolean = false
    private var isFeedInitialized: Boolean = false
    private val mutex = ReentrantLock()

    fun flush() {
        if (isConcurrent) {
            mutex.lock()
            writer.flush()
            mutex.unlock()
        } else {
            writer.flush()
        }
    }

    private fun write(data: String, flush: Boolean) {
        openDataFeedConnection()
        writer.write(data + "\n")
    }

    fun writeToFeed(data: String, flush: Boolean) {
        if (isConcurrent) {
            mutex.lock()
            write(data, flush)
            mutex.unlock()
        } else {
            write(data, flush)
        }
        if (flush) flush()
    }

    fun openDataFeedConnection() {
        if (!isFeedConnectionOpen) {
            try {
                if (!isFeedInitialized) {
                    initializeFeed()
                    isFeedInitialized = true
                }
            } catch (e: Exception) {
                e.printStackTrace()
                // the datafeed could be already opened and started, if so do nothing
            }
            socket = Socket(dataFeedIp, dataFeedPort)
            socket.sendBufferSize = 1_000_000
            outputStream = socket.getOutputStream()
            // writer = PrintWriter(outputStream, true)
            writer = BufferedWriter(OutputStreamWriter(outputStream), 1024 * 1024)
            isFeedConnectionOpen = true
        }
    }

    fun closeDataFeedConnection(closeRemote: Boolean = false) {
        if (this::writer.isInitialized) {
            writer.close()
            outputStream.close()
            socket.close()
            if (closeRemote) {
                stopFeed()
                dropFeed()
                isFeedInitialized = false
            }
            isFeedConnectionOpen = false
        }
    }

    fun stopFeed() = queryAsterixDB("USE $dataverse; STOP FEED $feedName;", blockOnError = false) // The feed could not exist yet

    fun createDataset(multiTs: Boolean): Boolean {
        var pk = "$TSID, $ID"
        if (multiTs) pk = "$TSID, $LABEL, $ID"
        return queryAsterixDB("USE $dataverse; CREATE DATASET $dataset($dataType) IF NOT EXISTS primary key $pk;")
    }

    fun createSpatialIndex(): Boolean {
        return queryAsterixDB("USE $dataverse; CREATE INDEX ${LOCATION}_$id on $dataset($LOCATION) type rtree;")
    }

    fun initializeFeed() : Boolean {
        var failures = 0
        do { // Until I have successfully created a DataFeed and I can actually connect to it
            var datafeedSetup = false
            var socketConnect = false
            try {
                stopFeed()
                datafeedSetup = setupDataFeed()
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
                "format": "adm",
                "policy": "spill"
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
>>>>>>> feat-tssingletable
            return false
        }
    }

<<<<<<< HEAD
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
=======
    private fun queryAsterixDB(sql: String, checkResults: Boolean = false, blockOnError: Boolean = true): Boolean {
        // println(sql)
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
>>>>>>> feat-tssingletable
                // Check if it returned something
                val isEmpty = Regex("\"results\"\\s*:\\s*\\[\\s*]").containsMatchIn(responseText)
                return !isEmpty
            }
<<<<<<< HEAD
        }
        else {
            println(responseText)
            return false
        }

=======
        } else {
            return false
        }
>>>>>>> feat-tssingletable
    }

    fun updateTs(upsertStatement: String) {
        queryAsterixDB(upsertStatement)
    }

<<<<<<< HEAD
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
                    return try {
                        val jsonResponse = JSONObject(responseText)
                        val resultArray = jsonResponse.getJSONArray("results")
                        if(!isGroupBy){
                            AsterixDBResult.SelectResult(resultArray)
                        }else{
                            AsterixDBResult.GroupByResult(resultArray)
                        }
                    } catch (e: Exception) {
                        println(e)
                        println("Error parsing JSON response: ${e.message}")
=======
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
>>>>>>> feat-tssingletable
                        throw Exception("Error parsing JSON response: ${e.message}")
                    }
                } else {
                    AsterixDBResult.InsertResult
                }
            }
<<<<<<< HEAD
            else -> {
                println("Query failed with status code $statusCode")
                throw Exception("Query failed with status code $statusCode")
            }
        }
    }

    fun deleteTs(deleteDataset: Boolean = true){
        val sqlStatement : String
        sqlStatement = if(deleteDataset) {
            """
            USE $dataverse;
            STOP FEED $feedName;
            DROP FEED $feedName;
            DROP dataset $dataset;
            """.trimIndent()
        }else{
            """ 
            USE $dataverse;
            STOP FEED $feedName;
            DROP FEED $feedName;
            """.trimIndent()
        }
        queryAsterixDB(sqlStatement)
=======

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

                CREATE TYPE $EVENT AS OPEN {
                    $TSID: bigint,
                    $ID: bigint,
                    $LABEL: int,
                    $LOCATION: geometry?,
                    $EDGES: [Edge]?
                };
            """.trimIndent()
        return queryAsterixDB(sql)
>>>>>>> feat-tssingletable
    }
}
