package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.*
import it.unibo.graph.query.Filter
import it.unibo.graph.query.Operators
import it.unibo.graph.utils.*
import org.json.JSONArray
import org.json.JSONObject
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.io.WKTWriter
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


sealed class AsterixDBResult {
    data class SelectResult(val entities: JSONArray) : AsterixDBResult()
    data class GroupByResult(val entities: JSONArray) : AsterixDBResult()
    object InsertResult : AsterixDBResult()
    object ErrorResult : AsterixDBResult()
}

fun checkAndparseTimestampToString(label: String, timestamp: Long): String {
    if (timestamp != Long.MAX_VALUE && timestamp != Long.MIN_VALUE) {
        return """"$label": datetime("${timestampToISO8601(timestamp)}"),"""
    } else {
        return ""
    }
}

fun hashMapToGeometry(location: Map<String, Any>): Geometry? {
    val type = location["type"] as? String ?: return null
    val coordinates = location["coordinates"] as? List<*> ?: return null

    val wkt = when (type.uppercase()) {
        "POINT" -> {
            val x = coordinates[0]
            val y = coordinates[1]
            val z = if (coordinates.size > 2) coordinates[2] else 0
            "POINT($x $y $z)"
        }

        "POLYGON" -> {
            val points = (coordinates[0] as List<List<Double>>)
                .joinToString(", ") { (lon, lat) -> "$lon $lat" }
            "POLYGON(($points))"
        }

        else -> return null
    }

    val reader = WKTReader()
    return reader.read(wkt)
}

fun parseProp(it: HashMap<*, *>, fromTimestamp: Long, toTimestamp: Long, key: String, g: Graph): P {
    fun parsePropType(key: String, value: Any): Pair<PropType, Any> {
        //TODO: HANDLING ONLY SIMPLE PROP TYPES
        return if (key == LOCATION) {
            Pair(PropType.GEOMETRY, hashMapToGeometry(value as HashMap<String, Any>)!!)
        } else {
            when (value) {
                is Int -> Pair(PropType.INT, value.toInt())
                is Long -> Pair(PropType.LONG, value.toLong())
                is Double -> Pair(PropType.DOUBLE, value.toDouble())
                is HashMap<*, *> -> {
                    if (it.keys.contains(listOf("type", "coordinates"))) {
                        Pair(PropType.GEOMETRY, hashMapToGeometry(value as HashMap<String, Any>)!!)
                    } else {
                        Pair(PropType.STRING, value.toString())
                    }
                }

                else -> Pair(PropType.STRING, value.toString())
            }
        }
    }

    val propType = parsePropType(key, it[key]!!)
    return P(
        DUMMY_ID,
        sourceId = DUMMY_ID.toLong(),
        key = key,
        value = propType.second,
        type = propType.first,
        sourceType = NODE,
        g = g,
        fromTimestamp = fromTimestamp,
        toTimestamp = toTimestamp
    )
}

fun timestampToISO8601(timestamp: Long): String {
    val instant = Instant.ofEpochMilli(timestamp)
    val formatter = DateTimeFormatter.ISO_INSTANT
    return when {
        timestamp <= 0 -> formatter.format(Instant.ofEpochMilli(0))
        timestamp == Long.MAX_VALUE -> formatter.format(Instant.ofEpochMilli(MAX_ASTERIX_DATE))
        else -> formatter.format(instant)
    }
}

fun dateToTimestamp(date: String): Long {
    val formatterWithMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss")
    val formatterWithMillis2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
    var localDateTime: LocalDateTime
    try {
        localDateTime = LocalDateTime.parse(date, formatterWithMillis2)
    }catch(e: Exception) {
         localDateTime = LocalDateTime.parse(date, formatterWithMillis)
    }


    val instant = localDateTime.toInstant(ZoneOffset.UTC)

    return instant.toEpochMilli()
}

private fun parsePropertyValue(value: JSONObject, type: PropType): Any {
    return when (type) {
        PropType.INT -> value.getInt("intValue")
        PropType.DOUBLE -> value.getDouble("doubleValue")
        PropType.STRING -> value.getString("stringValue")
        PropType.GEOMETRY -> WKTReader().read(value.getString("geometryValue"))
        else -> ""
    }
}


fun parseLocationToWKT(locationProp: P?, isUpdate: Boolean = false): String {
    return when (locationProp) {
        null -> ""
        else -> {
            if (isUpdate)
                """ "location": st_geom_from_text('${WKTWriter().write((locationProp.value as Geometry))}'),"""
            else
                """ "location": "${WKTWriter().write((locationProp.value as Geometry))}","""
        }
    }
}

fun relationshipToAsterixCitizen(rels: List<R>): String {
    return when (rels.size) {
        0 -> ""
        else -> """ "relationships": [${rels.map(::relToJson).joinToString(", ")}],"""
    }
}

fun propertiesToAsterixCitizen(props: List<P>): String {
    return when (props.size) {
        0 -> ""
        else -> """ "properties":  [${
            props.map(::propToJson).joinToString(", ")
        }], """ + propertyToAsterixCitizen(props)
    }
}

fun propertyToAsterixCitizen(props: List<P>): String {
    return when (props.size) {
        0 -> ""
        else -> props.map { """ "${it.key}" : ${parseValue(it.key, it.value)}""" }.joinToString(",\n ")
            .let { if (it.isNotEmpty()) "$it,\n" else "" }
    }
}


fun relToJson(relationship: R): String {
    val fromTimestampStr = checkAndparseTimestampToString("fromTimestamp", relationship.fromTimestamp)
    val toTimestampStr = checkAndparseTimestampToString("toTimestamp", relationship.toTimestamp)
    return """
        {
            "type": "${relationship.label}",
            "fromN": ${relationship.fromN},
            "toN": ${relationship.toN},
            $fromTimestampStr
            $toTimestampStr
            "properties": ${relationship.getProps().map(::propToJson)}
        }
        """
}

fun propToJson(property: P): String {
    val fromTimestampStr = checkAndparseTimestampToString("fromTimestamp", property.fromTimestamp)
    val toTimestampStr = checkAndparseTimestampToString("toTimestamp", property.toTimestamp)

    return """
        {
            "sourceId": ${property.sourceId},
            "sourceType": ${property.sourceType},
            "key": "${property.key}",
            "value": ${getPropertyValue(property.value, property.type)},
            $fromTimestampStr
            $toTimestampStr
            "type": ${property.type.ordinal}
        }
    """
}

fun jsonToProp(json: JSONObject, g: Graph): P {
    return P(
        id = DUMMY_ID,
        sourceId = json.getLong("sourceId"),
        sourceType = json.getBoolean(("sourceType")),
        key = json.getString("key"),
        value = parsePropertyValue(json.getJSONObject("value"), PropType.entries[json.getInt("type")]),
        type = PropType.entries[json.getInt("type")],
        fromTimestamp = if (json.has("fromTimestamp")) dateToTimestamp(json.getString("fromTimestamp")) else Long.MIN_VALUE,
        toTimestamp = if (json.has("toTimestamp")) dateToTimestamp(json.getString("toTimestamp")) else Long.MAX_VALUE,
        g = g
    )
}

fun jsonToRel(json: JSONObject, g: Graph): R {
    val newRelationship = R(
        id = DUMMY_ID,
        label = labelFromString(json.getString("type")),
        fromN = 0L, // Valore placeholder, se non è nel JSON
        toN = json.getLong("toN"),
        fromTimestamp = if (json.has("fromTimestamp")) dateToTimestamp(json.getString("fromTimestamp")) else Long.MIN_VALUE,
        toTimestamp = if (json.has("toTimestamp")) dateToTimestamp(json.getString("toTimestamp")) else Long.MAX_VALUE,
        g = g
    )
    newRelationship.properties.addAll(
        json.getJSONArray("properties")
            .let { array -> List(array.length()) { array.getJSONObject(it) } }
            .map { jsonToProp(it, g) }
    )
    return newRelationship
}

fun getPropertyValue(value: Any, valueType: PropType): JSONObject {
    return when (valueType) {
        PropType.DOUBLE -> return JSONObject().apply {
            put("doubleValue", value)
        }

        PropType.STRING -> return JSONObject().apply {
            put("stringValue", value)
        }

        PropType.INT -> return JSONObject().apply {
            put("intValue", value)
        }

        PropType.GEOMETRY -> return JSONObject().apply {
            put("geometryValue", WKTWriter().write(value as Geometry))
        }

        else -> JSONObject().apply {
            put("stringValue", value)
        }
    }
}

fun parseValue(property: String, value: Any): String {
    //TODO: Fix this, ci saranno altri tipi da parsare
    return when {
        //property.lowercase().contains("timestamp") -> """datetime("${timestampToISO8601(value as Long)}")"""
        value is Int -> "$value"
        value is Long -> "$value"
        else -> """ "$value" """
    }
}

fun applyFilters(filters: List<Filter>): String {
    return when {
        filters.isEmpty() -> ""
        else -> "WHERE ${filters.map(::parseFilter).joinToString(separator = "\n AND ")}"
    }
}

private fun parseFilter(filter: Filter): String {
    fun parseOperator(op: Operators): String = when (op) {
        Operators.EQ -> "="
        Operators.LT -> "<"
        Operators.GT -> ">"
        Operators.LTE -> "<="
        Operators.GTE -> ">="
        Operators.ST_CONTAINS -> "st_contains"
        Operators.ST_INTERSECTS -> "st_intersects"
    }

    fun escapeIdentifier(name: String): String = if (name == "value") "`value`" else name

    fun normalizeWkt(wkt: String): String? {
        val clean = wkt.trim().removeSurrounding("\"").removeSurrounding("'")
        val wktPattern = Regex(
            """^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(.*\)$""",
            RegexOption.IGNORE_CASE
        )
        return if (wktPattern.matches(clean)) clean else null
    }

    val property = filter.property
    val parsedValue = parseValue(property, filter.value)

    var left = if (filter.attrFirst) escapeIdentifier(property) else escapeIdentifier(parsedValue)
    var right = if (filter.attrFirst) escapeIdentifier(parsedValue) else escapeIdentifier(property)

    left = if (left == "fromTimestamp" || left == "toTimestamp") "timestamp" else left
    right = if (right == "fromTimestamp" || right == "toTimestamp") "timestamp" else right


    right = if(left == "timestamp" && right.toLong() < 0) "0" else right

    return when (filter.operator) {
        Operators.ST_CONTAINS, Operators.ST_INTERSECTS -> {
            val arg1Raw = remove3DfromWkt(if (filter.attrFirst) right else left)
            val arg2Raw = remove3DfromWkt(if (filter.attrFirst) left else right)


            val arg1 = normalizeWkt(arg1Raw)?.let { "st_geom_from_text('$it')" } ?: arg1Raw
            val arg2 = normalizeWkt(arg2Raw)?.let { "st_geom_from_text('$it')" } ?: arg2Raw

            "${parseOperator(filter.operator)}($arg1, $arg2)"
        }

        else -> "$left ${parseOperator(filter.operator)} $right"
    }
}