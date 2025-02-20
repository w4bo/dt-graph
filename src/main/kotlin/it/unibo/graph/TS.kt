package it.unibo.graph

import it.unibo.graph.structure.CustomVertex
import org.rocksdb.RocksDB
import java.net.HttpURLConnection
import java.net.URL
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import org.json.JSONArray
import org.json.JSONObject
import java.io.*
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime

interface TS : Serializable {
    fun getTSId(): Long
    fun add(label: String, timestamp: Long, value: Long) = add(
        N(
            encodeBitwise(getTSId(), timestamp),
            label,
            timestamp = timestamp,
            value = value,
            fromTimestamp = timestamp,
            toTimestamp = timestamp
        )
    )

    fun add(n: N): N
    fun getValues(): List<N>
    fun get(id: Long): N
}

class MemoryTS(val id: Long) : TS {
    private val values: MutableMap<Long, N> = mutableMapOf()

    override fun getTSId(): Long = id

    override fun add(n: N): N {
        values[n.timestamp!!] = n
        return n
    }

    override fun getValues(): List<N> = values.values.toList()

    override fun get(id: Long): N = values[decodeBitwise(id).second]!!
}

class RocksDBTS(val id: Long, val db: RocksDB) : TS {
    override fun getTSId(): Long = id

    override fun add(n: N): N {
        db.put("$id|${n.timestamp}".toByteArray(), serialize(n))
        return n
    }

    override fun getValues(): List<N> {
        val acc: MutableList<N> = mutableListOf()
        val iterator = db.newIterator()
        iterator.seek("$id|".toByteArray())
        while (iterator.isValid) {
            val key = String(iterator.key())
            if (!key.startsWith("$id|")) break
            acc += deserialize<N>(iterator.value())
            iterator.next()
        }
        return acc
    }

    override fun get(timestamp: Long): N = deserialize(db.get("$id|${timestamp}".toByteArray()))
}

class CustomTS(ts: TS) : TS by ts {
    override fun add(label: String, timestamp: Long, value: Long): N {
        return add(
            CustomVertex(
                encodeBitwise(getTSId(), timestamp),
                label,
                timestamp = timestamp,
                value = value,
                fromTimestamp = timestamp,
                toTimestamp = timestamp
            )
        )
    }
}

class AsterixDBTS(val id: Long, private val dbHost: String, private val dataverse: String, private val dataset: String): TS {

    override fun getTSId(): Long = id

    override fun add(n: N): N {
        val insertQuery = """
        USE $dataverse;
        UPSERT INTO $dataset ([
            {
                "id": "$id|${n.timestamp}",
                "timestamp": datetime("${convertTimestampToISO8601(n.id)}"),
                "property": "${n.type}",
                "location": point("${n.location?.toString()?.replace("(", "")?.replace(")", "") ?: "12.23593,44.147788"}"),
                "relationships": ${n.getRels().map(::relToJson)},
                "fromTimestamp": datetime("${convertTimestampToISO8601(n.fromTimestamp)}"),
                "toTimestamp": datetime("${convertTimestampToISO8601(n.toTimestamp)}"),
                "value": ${n.value}
            }
        ]);
        """.trimIndent()
        println("HELLOO")
        println("${n.fromTimestamp}")
        println("${n.toTimestamp}")
        val result = queryAsterixDB(insertQuery)
        //TODO: Handle negative results
        return n
    }

    override fun getValues(): List<N> {
        val selectQuery = """
        USE $dataverse;
        SELECT * FROM $dataset
        WHERE REGEXP_CONTAINS(id, "^$id.*");
        """.trimIndent()
        when (val result = queryAsterixDB(selectQuery)) {
            is AsterixDBResult.SelectResult -> {
                return result.entities
            }
            else -> {
                println("Error occurred while performing query \n $selectQuery")
                return emptyList()
            }
        }
    }

    override fun get(timestamp: Long): N {
        val selectQuery = """
        USE $dataverse;
        SELECT * FROM $dataset
        WHERE id = '$id|$timestamp'
        """.trimIndent()
        val result = queryAsterixDB(selectQuery)
        when (result) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.size > 0){
                    return result.entities[0]
                }else{
                    //TODO: Fix this random return
                    return CustomVertex(id=-1, timestamp=timestamp, fromTimestamp = -1, toTimestamp = -1, type="Error")
                }

            }
            else -> {
                println("Error occurred while performing query \n $selectQuery")
                //TODO fix this empty node
                return CustomVertex(id=-1, timestamp=timestamp, fromTimestamp = -1, toTimestamp = -1, type="Error")
            }
        }
    }

    sealed class AsterixDBResult {
        data class SelectResult(val entities: List<N>) : AsterixDBResult()
        object InsertResult : AsterixDBResult()
        object ErrorResult : AsterixDBResult()
    }

    private fun queryAsterixDB(queryStatement: String): AsterixDBResult {
        val connection = URL(dbHost).openConnection() as HttpURLConnection
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
                        val entities = mutableListOf<N>()
                        for (i in 0 until resultArray.length()) {
                            val jsonEntity = resultArray.getJSONObject(i).getJSONObject(dataset)


                            val entity = CustomVertex(
                                id = encodeBitwise(getTSId(), jsonEntity.getString("id").split("|")[1].toLong()),
                                timestamp = dateToTimestamp(jsonEntity.getString("timestamp")),
                                type = jsonEntity.getString("property"),
                                location = parseLocation(jsonEntity.getJSONArray("location")),
                                fromTimestamp = dateToTimestamp(jsonEntity.getString("fromTimestamp")) ,
                                toTimestamp = dateToTimestamp(jsonEntity.getString("toTimestamp")),
                                value = jsonEntity.getDouble("value").toLong()
                            )
                            entity.relationships.addAll(
                                jsonEntity.getJSONArray("relationships")
                                    .let { array -> List(array.length()) { array.getJSONObject(it) } }
                                    .map(::jsonToRel)
                            )
                            entities.add(entity)
                        }
                        AsterixDBResult.SelectResult(entities)
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
    private fun relToJson(relationship: R): String {
        return JSONObject().apply {
            put("id", relationship.id)
            put("type", relationship.type)
            put("toN", relationship.toN)
            put("fromNextRel", relationship.fromNextRel)
            put("toNextRel", relationship.toNextRel)
        }.toString()
    }

    private fun jsonToRel(json: JSONObject): R{
        return R(
            id = json.getInt("id"),
            type = json.getString("type"),
            fromN = 0L, // Valore placeholder, se non è nel JSON
            toN = json.getLong("toN"),
            fromNextRel = if (json.has("fromNextRel")) json.optInt("fromNextRel", -1).takeIf { it != -1 } else null,
            toNextRel = if (json.has("toNextRel")) json.optInt("toNextRel", -1).takeIf { it != -1 } else null
        )
    }
    private fun parseLocation(location: JSONArray): Pair<Double, Double> {
        return Pair(location.getDouble(0), location.getDouble(0))
    }
    private fun convertTimestampToISO8601(timestamp: Long): String {
        val instant = Instant.ofEpochMilli(timestamp)
        val formatter = DateTimeFormatter.ISO_INSTANT
        return formatter.format(instant)
    }
    private fun dateToTimestamp(date: String): Long{
        val formatterWithMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        val localDateTime = LocalDateTime.parse(date, formatterWithMillis)

        val instant = localDateTime.toInstant(ZoneOffset.UTC)

        return instant.toEpochMilli()
    }

}