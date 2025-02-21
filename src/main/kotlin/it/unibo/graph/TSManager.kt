package it.unibo.graph

import org.rocksdb.Options
import org.rocksdb.RocksDB
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

interface TSManager {
    fun addTS(): TS
    fun nextTSId(): Long
    fun getTS(id: Long): TS
    fun clear()
}

class MemoryTSManager : TSManager {
    private val tss: MutableList<TS> = ArrayList()

    override fun getTS(id: Long): TS {
        return tss[(id as Number).toInt()]
    }

    override fun addTS(): TS {
        val ts = CustomTS(MemoryTS(nextTSId()))
        tss += ts
        return ts
    }

    override fun nextTSId(): Long = tss.size.toLong()

    override fun clear() {
        tss.clear()
    }
}

class RocksDBTSM : TSManager {
    val db: RocksDB
    val DB_NAME = "db_ts"
    var id = 0

    init {
        val options = Options()
        options.setCreateIfMissing(true)
        options.setCreateMissingColumnFamilies(true)
        db = RocksDB.open(options, DB_NAME)
    }

    override fun getTS(id: Long): TS {
        return CustomTS(RocksDBTS(id, db))
    }

    override fun addTS(): TS {
        return CustomTS(RocksDBTS(nextTSId(), db))
    }

    override fun nextTSId(): Long = id++.toLong()

    override fun clear() {
        val iterator = db.newIterator()
        iterator.seekToFirst()
        while (iterator.isValid) {
            db.delete(iterator.key())
            iterator.next()
        }
        id = 0
    }
}

class AsterixDBTSM private constructor(
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
        return CustomTS(AsterixDBTS(nextTSId(), dbHost, dataverse, dataset))
    }

    override fun nextTSId(): Long = id++.toLong()

    override fun getTS(id: Long): TS {
        return CustomTS(AsterixDBTS(id, dbHost, dataverse, dataset))
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
          CREATE TYPE NodeRelationship AS CLOSED {
              id: int,
              `type`: string,
              toN: bigint,
              fromNextRel: int?,
              toNextRel: int?
          };
          CREATE TYPE $datatype AS OPEN {
              id: STRING,
              timestamp: DATETIME,
              property: STRING,
              location: POINT,
              relationships: [NodeRelationship],
              fromTimestamp: DATETIME,
              toTimestamp: DATETIME,
              `value`: FLOAT
          };

          CREATE DATASET $dataset($datatype)IF NOT EXISTS primary key id;
          create index measurement_location on $dataset(location) type rtree;
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
        fun createDefault(): AsterixDBTSM {
            return AsterixDBTSM(
                "localhost",
                "19002",
                "Measurements_Dataverse",
                "Measurement",
                "OpenMeasurements"
            )
        }

    }

}
