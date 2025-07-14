package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
import it.unibo.graph.structure.CustomTS
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
    nodeControllersIPs: List<String>,
    val datatype: String,
) : TSManager {

    private class UniformSampler<T>(private val source: List<T>) {
        private var pool: MutableList<T> = source.shuffled().toMutableList()
        private var index = 0

        fun next(): T {
            if (pool.isEmpty()) throw IllegalArgumentException("Source list is empty")

            // Reset and reshuffle once we've cycled through the list
            if (index >= pool.size) {
                pool = source.shuffled().toMutableList()
                index = 0
            }

            return pool[index++]
        }
    }

    private val nodeControllersPool = UniformSampler(nodeControllersIPs)
    val clusterControllerHost: String = "http://$host:$port/query/service"
    var id = 1
    val busyPorts: MutableSet<Int> = mutableSetOf()
    val tsList: MutableMap<Long, CustomTS> = mutableMapOf()

    init {
        setupAsterixDB()
    }
    fun addTS(inputPath : String): TS {
        val tsId = nextTSId()
        val newTS =
            AsterixDBTS(g, tsId, clusterControllerHost, nodeControllersPool.next(), dataverse, datatype, busyPorts, inputPath = inputPath)
        val outTS = CustomTS(newTS, g)
        tsList.put(tsId, outTS)
        busyPorts.add(newTS.dataFeedPort)
        return outTS
    }
    override fun addTS(): TS {
        val tsId = nextTSId()
        val newTS =
            AsterixDBTS(g, tsId, clusterControllerHost, nodeControllersPool.next(), dataverse, datatype, busyPorts)
        val outTS = CustomTS(newTS, g)
        tsList.put(tsId, outTS)
        busyPorts.add(newTS.dataFeedPort)
        return outTS
    }

    override fun nextTSId(): Long = id++.toLong()

    override fun getTS(id: Long): TS {
        return tsList.getValue(id)
    }

    override fun clear() {
        id = 1
        tsList.values.forEach { (it.ts as AsterixDBTS).deleteTs() }
    }

    // Private utility functions
    private fun queryAsterixDB(host: String, query: String): Boolean {
        val uri = URI(host)
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
            "${URLEncoder.encode(it.key, StandardCharsets.UTF_8.name())}=${
                URLEncoder.encode(
                    it.value,
                    StandardCharsets.UTF_8.name()
                )
            }"
        }

        connection.outputStream.use { it.write(postData.toByteArray()) }

        try {
            connection.inputStream.bufferedReader().use { it.readText() }
            return true
        } catch (e: Exception) {
            connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
            throw UnsupportedOperationException(e)
        }
    }

    private fun setupAsterixDB() {
        val creationQuery = """
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
              properties: [Property]?,
              fromTimestamp: DATETIME,
              toTimestamp: DATETIME
          };
            """.trimIndent()
        if (!queryAsterixDB(clusterControllerHost, creationQuery)) {
            println("Something went wrong while creating time series environment")
        }
    }

    companion object {
        fun createDefault(g: Graph): AsterixDBTSM {
            return AsterixDBTSM(
                g,
                props.get("default_cc_host").toString(),
                props.get("default_cc_port").toString(),
                props.get("default_dataverse").toString(),
                listOf(props.get("default_nc_pool").toString()),
                props.get("default_datatype").toString(),
            )
        }
    }
}
