package it.unibo.graph

import it.unibo.graph.structure.CustomVertex
import org.json.JSONObject
import org.rocksdb.RocksDB
import java.io.Serializable
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import org.locationtech.jts.io.geojson.GeoJsonWriter

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
            UPSERT INTO $dataset ([{
                "id": "$id|${n.timestamp}",
                "timestamp": datetime("${convertTimestampToISO8601(n.timestamp!!)}"),
                ${n.nextRel?.let { "\"nextRel\": \"$it\"," } ?: ""}
                ${n.nextProp?.let { "\"nextProp\": \"$it\"," } ?: ""}
                "property": "${n.type}",
                "location": st_geom_from_geojson(${n.location?.let { GeoJsonWriter().write(it) } ?: "{\"coordinates\":[11.799328,44.235394],\"type\":\"Point\"}"}),
                "relationships": [${n.getRels().map(::relToJson).joinToString(", ")}],
                "properties": [${n.getProps().map(::propToJson).joinToString(", ")}],
                "fromTimestamp": datetime("${convertTimestampToISO8601(n.fromTimestamp)}"),
                "toTimestamp": datetime("${convertTimestampToISO8601(n.toTimestamp)}"),
                "value": ${n.value}
            }]);
        """.trimIndent()

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
                    return CustomVertex(id =-1, timestamp =timestamp, fromTimestamp = -1, toTimestamp = -1, type ="Error")
                }

            }
            else -> {
                println("Error occurred while performing query \n $selectQuery")
                //TODO fix this empty node
                return CustomVertex(id =-1, timestamp =timestamp, fromTimestamp = -1, toTimestamp = -1, type ="Error")
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
                                location = jsonEntity.getJSONObject("location").toString(),
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
                            entity.properties.addAll(
                                jsonEntity.getJSONArray("properties")
                                    .let { array -> List(array.length()) { array.getJSONObject(it) } }
                                    .map(::jsonToProp)
                            )
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
        val fromTimestampStr = checkAndparseTimestampToString("fromTimestamp", relationship.fromTimestamp)
        val toTimestampStr = checkAndparseTimestampToString("toTimestamp", relationship.toTimestamp)
        return """
        {
            "id": ${relationship.id},
            "type": "${relationship.type}",
            "fromN": ${relationship.fromN},
            "toN": ${relationship.toN},
            $fromTimestampStr
            $toTimestampStr
            "properties": ${relationship.getProps().map(::propToJson)}
        }
    """.trimIndent()
    }

    private fun jsonToRel(json: JSONObject): R{
        val newRelationship = R(
            id = json.getInt("id"),
            type = json.getString("type"),
            fromN = 0L, // Valore placeholder, se non è nel JSON
            toN = json.getLong("toN"),
            fromTimestamp =  if (json.has("fromTimestamp")) dateToTimestamp(json.getString("fromTimestamp")) else Long.MIN_VALUE,
            toTimestamp =  if (json.has("toTimestamp")) dateToTimestamp(json.getString("toTimestamp")) else Long.MAX_VALUE,
        )
        newRelationship.properties.addAll(
            json.getJSONArray("properties")
                .let { array -> List(array.length()) { array.getJSONObject(it) } }
                .map(::jsonToProp)
        )
        return newRelationship
    }

    private fun jsonToProp(json: JSONObject): P {
        return P(
            id = json.getInt("id"),
            sourceId = json.getLong("sourceId"),
            sourceType = json.getBoolean(("sourceType")),
            key = json.getString("key"),
            value = parsePropertyValue(json.getJSONObject("value"),PropType.entries[json.getInt("type")]),
            type = PropType.entries[json.getInt("type")],
            fromTimestamp =  if (json.has("fromTimestamp")) dateToTimestamp(json.getString("fromTimestamp")) else Long.MIN_VALUE,
            toTimestamp =  if (json.has("toTimestamp")) dateToTimestamp(json.getString("toTimestamp")) else Long.MAX_VALUE,
        )
    }
    private fun parsePropertyValue(value:JSONObject, type:PropType): Any {
     return when (type){
         PropType.INT -> value.getInt("intValue")
         PropType.DOUBLE -> value.getDouble("doubleValue")
         PropType.STRING -> value.getString("stringValue")
         else -> ""
     }
    }
    private fun propToJson(property: P): String {
        val fromTimestampStr = checkAndparseTimestampToString("fromTimestamp", property.fromTimestamp)
        val toTimestampStr = checkAndparseTimestampToString("toTimestamp", property.toTimestamp)

        return """
        {
            "id": ${property.id},
            "sourceId": ${property.sourceId},
            "sourceType": ${property.sourceType},
            "key": "${property.key}",
            "value": ${getPropertyValue(property.value, property.type)},
            $fromTimestampStr
            $toTimestampStr
            "type": ${property.type.ordinal}
        }
    """.trimIndent()
    }


    private fun getPropertyValue(value: Any, valueType: PropType) : JSONObject {
        return when (valueType) {
            PropType.DOUBLE -> return JSONObject().apply{
                put("doubleValue", value)
            }
            PropType.STRING -> return JSONObject().apply{
                put("stringValue",value)
            }
            PropType.INT -> return JSONObject().apply{
                put("intValue",value)
            }
            else -> JSONObject().apply{
                put("stringValue", value)
            }
        }
    }
    private fun checkAndparseTimestampToString(label: String, timestamp: Long): String{
        if (timestamp != Long.MAX_VALUE && timestamp != Long.MIN_VALUE) {
            return """"$label": datetime("${convertTimestampToISO8601(timestamp)}"),"""
        } else {
            return ""
        }
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