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
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


sealed class AsterixDBResult {
    data class SelectResult(val entities: JSONArray) : AsterixDBResult()
    data class GroupByResult(val entities: JSONArray) : AsterixDBResult()
    object InsertResult : AsterixDBResult()
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
            val points = (coordinates[0] as List<List<Double>>).joinToString(",") { (lon, lat) -> "$lon $lat" }
            "POLYGON(($points))"
        }

        else -> throw IllegalArgumentException("Unknown geometry type: $type")
    }
    val reader = WKTReader()
    return reader.read(wkt)
}

fun parseProp(it: HashMap<*, *>, fromTimestamp: Long, toTimestamp: Long, key: String, g: Graph): P {
    fun parsePropType(key: String, value: Any): Pair<PropType, Any> {
        return if (key == LOCATION) {
            Pair(PropType.GEOMETRY, hashMapToGeometry(value as HashMap<String, Any>)!!)
        } else {
            when (value) {
                is Int -> Pair(PropType.INT, value)
                is Long -> Pair(PropType.LONG, value)
                is Double -> Pair(PropType.DOUBLE, value)
                is HashMap<*, *> -> {
                    if (it.keys.contains(listOf("type","coordinates"))) {
                        Pair(PropType.GEOMETRY, hashMapToGeometry(value as HashMap<String, Any>)!!)
                    } else {
                        Pair(PropType.STRING, value.toString())
                    }
                }

                is String -> Pair(PropType.STRING, value)
                else -> throw IllegalArgumentException("Unrecognized prop type: $value")
            }
        }
    }

    val propType = parsePropType(key, it[key]!!)
    return P(
        DUMMY_ID,
        sourceId = DUMMY_ID,
        key = key,
        value = propType.second,
        type = propType.first,
        sourceType = NODE,
        g = g,
        fromTimestamp = fromTimestamp,
        toTimestamp = toTimestamp
    )
}

fun dateToTimestamp(date: String): Long {
    val formatterWithMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss")
    val formatterWithMillis2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
    var localDateTime: LocalDateTime
    try {
        localDateTime = LocalDateTime.parse(date, formatterWithMillis2)
    } catch (_: Exception) {
        localDateTime = LocalDateTime.parse(date, formatterWithMillis)
    }
    val instant = localDateTime.toInstant(ZoneOffset.UTC)
    return instant.toEpochMilli()
}

private fun jsonToValue(value: Any, type: PropType): Any {
    return when (type) {
        PropType.GEOMETRY -> WKTReader().read(value as String)
        else -> value
    }
}

fun parseLocationToWKT(locationProp: P?, isUpdate: Boolean = false): String {
    return when (locationProp) {
        null -> ""
        else -> {
            if (isUpdate)
                """"$LOCATION": st_geom_from_text('${WKTWriter().write((locationProp.value as Geometry))}'),"""
            else
                """"$LOCATION": "${WKTWriter().write((locationProp.value as Geometry))}","""
        }
    }
}

fun edgesToAsterixCitizen(edges: List<R>): String {
    return when (edges.size) {
        0 -> ""
        else -> """"$EDGES": [${edges.joinToString(",", transform = ::edgeToJson)}],"""
    }
}

fun propertiesToAsterixCitizen(props: List<P>): String {
    return when (props.size) {
        0 -> ""
        else -> """"$PROPERTIES": [${props.joinToString(",", transform = ::propToJson)}], """ + propertyToAsterixCitizen(props)
    }
}

fun propertyToAsterixCitizen(props: List<P>): String {
    return when (props.size) {
        0 -> ""
        else -> props.joinToString(",") { """"${it.key}": ${valueToJson(it.value)}""" }.let { if (it.isNotEmpty()) "$it," else "" }
    }
}

fun edgeToJson(edge: R): String {
    return """{
            "$LABEL": "${edge.label}",
            "fromN": ${edge.fromN},
            "toN": ${edge.toN},
            "$PROPERTIES": ${edge.properties.map(::propToJson)}
        }""".trimIndent()
}

fun propToJson(property: P): String {
    return """{
            "$KEY": "${property.key}",
            "$VALUE": ${valueToJson(property.value)},
            "$TYPE": ${property.type.ordinal}
        }""".trimIndent()
}

fun jsonToProp(json: JSONObject, sourceId: Long, sourceType: Boolean, fromTimestamp: Long, toTimestamp: Long, g: Graph): P {
    return P(
        id = DUMMY_ID,
        sourceId = sourceId,
        sourceType = sourceType,
        key = json.getString(KEY),
        value = jsonToValue(json.get(VALUE), PropType.entries[json.getInt(TYPE)]),
        type = PropType.entries[json.getInt(TYPE)],
        fromTimestamp = fromTimestamp,
        toTimestamp = toTimestamp,
        g = g
    )
}

fun jsonToEdge(json: JSONObject, fromTimestamp: Long, toTimestamp: Long, g: Graph): R {
    val id = DUMMY_ID
    val newEdge = R(id = id, label = labelFromString(json.getString(LABEL)), fromN = json.getLong("fromN"), toN = json.getLong("toN"), fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)
    newEdge.properties.addAll(
        json.getJSONArray(PROPERTIES)
            .let { array -> List(array.length()) { array.getJSONObject(it) } }
            .map { jsonToProp(it, id, EDGE, fromTimestamp, toTimestamp, g) }
    )
    return newEdge
}

fun valueToJson(value: Any): String {
    return when (value) {
        is Int -> "$value"
        is Long -> "$value"
        is Double -> "$value"
        is Geometry -> "\"${WKTWriter().write(value)}\""
        else -> "\"$value\""
        // else -> throw IllegalArgumentException("Unknown prop type ${value::class.qualifiedName}")
    }
}

fun applyFilters(filters: List<Filter>): String {
    return when {
        filters.isEmpty() -> ""
        else -> "WHERE ${filters.joinToString(separator = "\n AND ", transform = ::parseFilter)}"
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

    fun escapeIdentifier(name: String): String = if (name == VALUE) "`$VALUE`" else name

    fun normalizeWkt(wkt: String): String? {
        val clean = wkt.trim().removeSurrounding("\"").removeSurrounding("'")
        val wktPattern = Regex(
            """^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(.*\)$""",
            RegexOption.IGNORE_CASE
        )
        return if (wktPattern.matches(clean)) clean else null
    }

    val property = filter.property
    val parsedValue = valueToJson(filter.value)
    var left = if (filter.attrFirst) escapeIdentifier(property) else escapeIdentifier(parsedValue)
    var right = if (filter.attrFirst) escapeIdentifier(parsedValue) else escapeIdentifier(property)
    right = if (left == ID && right.toLong() < 0) "0" else right

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