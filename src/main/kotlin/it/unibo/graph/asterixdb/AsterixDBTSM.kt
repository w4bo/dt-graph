package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
import it.unibo.graph.structure.CustomTS
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

class AsterixDBTSM private constructor(
    override val g: Graph,
    host: String,
    port: String,
    val dataverse: String,
    val datatype: String,
    val dataset:String
) : TSManager {
    val dbHost: String = "http://$host:$port/query/service"
    var id = 1

    init {
        createDataset(dataset)
    }

    override fun addTS(): TS {
        return CustomTS(AsterixDBTS(g, nextTSId(), dbHost, dataverse, dataset), g)
    }

    override fun nextTSId(): Long = id++.toLong()

    override fun getTS(id: Long): TS {
        return CustomTS(AsterixDBTS(g, id, dbHost, dataverse, dataset), g)
    }

    override fun clear() {
        id = 1
        deleteDataset(dataset)
    }

    // Private utility functions
    fun queryAsterixDB(host: String, query: String): Boolean {
        val url = URL(host)
        val connection = url.openConnection() as HttpURLConnection
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

        // Legge la risposta
        val responseText = try {
            connection.inputStream.bufferedReader().use { it.readText() }
        } catch (e: Exception) {
            connection.errorStream?.bufferedReader()?.use { it.readText() } ?: "Unknown error"
        }

        return true
    }

    private fun createDataset(dataset: String) {
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
              id: int,
              sourceId: bigint,
              sourceType: Boolean,
              `key`: string,
              `value`: PropertyValue,
              `type`: int,
              fromTimestamp: DATETIME?,
              toTimestamp: DATETIME?
          };

          CREATE TYPE NodeRelationship AS CLOSED {
              id: int,
              `type`: string,
              fromN: bigint,
              toN: bigint,
              fromTimestamp: DATETIME?,
              toTimestamp: DATETIME?,
              properties: [Property]?
          };
       
          CREATE TYPE Measurement AS OPEN {
              id: STRING,
              timestamp: DATETIME,
              property: STRING,
              location: geometry?,
              relationships: [NodeRelationship]?,
              properties: [Property]?,
              fromTimestamp: DATETIME,
              toTimestamp: DATETIME,
              `value`: FLOAT
          };
          
          CREATE DATASET $dataset($datatype)IF NOT EXISTS primary key id;
          create index measurement_location on $dataset(location) type rtree;
          
          DROP FEED MeasurementsFeed IF EXISTS;
          CREATE feed MeasurementsFeed WITH {
              "adapter-name": "socket_adapter",
              "sockets": "127.0.0.1:10001",
              "address-type": "IP",
              "type-name": "Measurement",
              "format": "adm"
          };
          CONNECT FEED MeasurementsFeed TO DATASET OpenMeasurements;
        
          start feed MeasurementsFeed;
            """.trimIndent()
        if (! queryAsterixDB(dbHost, creationQuery)){
            println("Something went wrong while creating dataset $dataset")
        }
    }

    private fun deleteDataset(dataset: String) {
        val deletionQuery = """
            USE $dataverse;
            DELETE FROM $dataset
            """.trimIndent()
        if (! queryAsterixDB(dbHost, deletionQuery)){
            println("Something went wrong while creating dataset $dataset")
        }
    }

    companion object {
        fun createDefault(g: Graph): AsterixDBTSM {
            return AsterixDBTSM(
                g,
                "localhost",
                "19002",
                "Measurements_Dataverse",
                "Measurement",
                "OpenMeasurements"
            )
        }
    }
}
