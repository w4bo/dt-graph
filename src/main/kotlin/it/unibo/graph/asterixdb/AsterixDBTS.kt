package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.*
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.structure.CustomVertex
import it.unibo.graph.utils.encodeBitwise
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class AsterixDBTS(override val g: Graph, val id: Long, private val dbHost: String, private val dataverse: String, private val dataset: String): TS {

    override fun getTSId(): Long = id

    override fun add(n: N): N {
        val insertQuery = """
            USE $dataverse;
            UPSERT INTO $dataset ([{
                "id": "$id|${n.timestamp}",
                "timestamp": datetime("${convertTimestampToISO8601(n.timestamp!!)}"),
                ${n.nextRel?.let { "\"nextRel\": \"$it\"," } ?: ""}
                ${n.nextProp?.let { "\"nextProp\": \"$it\"," } ?: ""}
                "property": "${n.label}",
                "location": st_geom_from_geojson(
                    ${n.getProps(name = "location").firstOrNull()?.value
                            ?.let { it as? String }
                            ?.takeIf { it.isNotBlank() }
                            ?: "{\"coordinates\":[0, 0],\"type\":\"Point\"}"}
                ),
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

    override fun getValues(by: List<Aggregate>, filter: List<Filter>): List<N> {
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
                if (result.entities.size > 0) {
                    return result.entities[0]
                } else {
                    //TODO: Fix this random return
                    throw IllegalArgumentException()
                    // return CustomVertex(id =-1, timestamp =timestamp, fromTimestamp = -1, toTimestamp = -1, type = "Error", g = g)
                }
            }

            else -> {
                //TODO fix this empty node
                throw IllegalArgumentException("Error occurred while performing query \n $selectQuery")
                // return CustomVertex(id =-1, timestamp =timestamp, fromTimestamp = -1, toTimestamp = -1, type = "Error", g = g)
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
                                type = labelFromString(jsonEntity.getString("property")),
                                fromTimestamp = dateToTimestamp(jsonEntity.getString("fromTimestamp")) ,
                                toTimestamp = dateToTimestamp(jsonEntity.getString("toTimestamp")),
                                value = jsonEntity.getDouble("value").toLong(),
                                g = g
                            )
                            entity.relationships.addAll(
                                jsonEntity.getJSONArray("relationships")
                                    .let { array -> List(array.length()) { array.getJSONObject(it) } }
                                    .map(::jsonToRel)
                            )
                            entity.properties.addAll(
                                jsonEntity.getJSONArray("properties")
                                    .let { array -> List(array.length()) { array.getJSONObject(it) } }
                                    .map(::jsonToProp)
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
        val fromTimestampStr = checkAndparseTimestampToString("fromTimestamp", relationship.fromTimestamp)
        val toTimestampStr = checkAndparseTimestampToString("toTimestamp", relationship.toTimestamp)
        return """
        {
            "id": ${relationship.id},
            "type": "${relationship.label}",
            "fromN": ${relationship.fromN},
            "toN": ${relationship.toN},
            $fromTimestampStr
            $toTimestampStr
            "properties": ${relationship.getProps().map(::propToJson)}
        }
    """.trimIndent()
    }

    private fun jsonToRel(json: JSONObject): R {
        val newRelationship = R(
            id = json.getInt("id"),
            label = enumValueOf<Labels>(json.getString("type")),
            fromN = 0L, // Valore placeholder, se non è nel JSON
            toN = json.getLong("toN"),
            fromTimestamp =  if (json.has("fromTimestamp")) dateToTimestamp(json.getString("fromTimestamp")) else Long.MIN_VALUE,
            toTimestamp =  if (json.has("toTimestamp")) dateToTimestamp(json.getString("toTimestamp")) else Long.MAX_VALUE,
            g = g
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
            value = parsePropertyValue(json.getJSONObject("value"), PropType.entries[json.getInt("type")]),
            type = PropType.entries[json.getInt("type")],
            fromTimestamp =  if (json.has("fromTimestamp")) dateToTimestamp(json.getString("fromTimestamp")) else Long.MIN_VALUE,
            toTimestamp =  if (json.has("toTimestamp")) dateToTimestamp(json.getString("toTimestamp")) else Long.MAX_VALUE,
            g = g
        )
    }
    private fun parsePropertyValue(value: JSONObject, type: PropType): Any {
        return when (type){
            PropType.INT -> value.getInt("intValue")
            PropType.DOUBLE -> value.getDouble("doubleValue")
            PropType.STRING -> value.getString("stringValue")
            PropType.GEOMETRY -> value.getString("geometryValue")
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
            PropType.GEOMETRY -> return JSONObject().apply{
                put("geometryValue", value.toString())
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