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
<<<<<<< HEAD
import java.time.Instant
=======
import org.locationtech.jts.io.geojson.GeoJsonReader
>>>>>>> feat-tssingletable
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


sealed class AsterixDBResult {
    data class SelectResult(val entities: JSONArray) : AsterixDBResult()
    data class GroupByResult(val entities: JSONArray) : AsterixDBResult()
    object InsertResult : AsterixDBResult()
<<<<<<< HEAD
    // object ErrorResult : AsterixDBResult()
}

fun checkAndparseTimestampToString(label: String, timestamp: Long): String {
    return if (timestamp != Long.MAX_VALUE && timestamp != Long.MIN_VALUE) {
        """"$label": datetime("${timestampToISO8601(timestamp)}"),"""
    } else {
        ""
    }
=======
>>>>>>> feat-tssingletable
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
<<<<<<< HEAD
            val points = (coordinates[0] as List<List<Double>>)
                .joinToString(", ") { (lon, lat) -> "$lon $lat" }
            "POLYGON(($points))"
        }

        else -> return null
    }

=======
            val points = (coordinates[0] as List<List<Double>>).joinToString(",") { (lon, lat) -> "$lon $lat" }
            "POLYGON(($points))"
        }

        else -> throw IllegalArgumentException("Unknown geometry type: $type")
    }
>>>>>>> feat-tssingletable
    val reader = WKTReader()
    return reader.read(wkt)
}

fun parseProp(it: HashMap<*, *>, fromTimestamp: Long, toTimestamp: Long, key: String, g: Graph): P {
    fun parsePropType(key: String, value: Any): Pair<PropType, Any> {
<<<<<<< HEAD
        //TODO: HANDLING ONLY SIMPLE PROP TYPES
=======
>>>>>>> feat-tssingletable
        return if (key == LOCATION) {
            Pair(PropType.GEOMETRY, hashMapToGeometry(value as HashMap<String, Any>)!!)
        } else {
            when (value) {
                is Int -> Pair(PropType.INT, value)
                is Long -> Pair(PropType.LONG, value)
                is Double -> Pair(PropType.DOUBLE, value)
                is HashMap<*, *> -> {
<<<<<<< HEAD
                    if (it.keys.contains(listOf("type", "coordinates"))) {
=======
                    if (it.keys.contains(listOf("type","coordinates"))) {
>>>>>>> feat-tssingletable
                        Pair(PropType.GEOMETRY, hashMapToGeometry(value as HashMap<String, Any>)!!)
                    } else {
                        Pair(PropType.STRING, value.toString())
                    }
                }

<<<<<<< HEAD
                else -> Pair(PropType.STRING, value.toString())
=======
                is String -> Pair(PropType.STRING, value)
                else -> throw IllegalArgumentException("Unrecognized prop type: $value")
>>>>>>> feat-tssingletable
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

<<<<<<< HEAD
fun timestampToISO8601(timestamp: Long): String {
    val instant = Instant.ofEpochMilli(timestamp)
    val formatter = DateTimeFormatter.ISO_INSTANT
    return when {
        timestamp <= 0 -> formatter.format(Instant.ofEpochMilli(0))
        timestamp == Long.MAX_VALUE -> formatter.format(Instant.ofEpochMilli(MAX_ASTERIX_DATE))
        else -> formatter.format(instant)
    }
}

=======
>>>>>>> feat-tssingletable
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

<<<<<<< HEAD
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
        else -> """ "relationships": [${rels.joinToString(", ", transform = ::relToJson)}],"""
    }
}

fun propertiesToAsterixCitizen(props: List<P>): String {
    return when (props.size) {
        0 -> ""
        else -> """ "properties":  [${
            props.joinToString(", ", transform = ::propToJson)
        }], """ + propertyToAsterixCitizen(props)
    }
}

fun propertyToAsterixCitizen(props: List<P>): String {
    return when (props.size) {
        0 -> ""
        else -> props.joinToString(",\n ") { """ "${it.key}" : ${parseValue(it.value)}""" }
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

fun parseValue(value: Any): String {
    //TODO: Fix this, ci saranno altri tipi da parsare
    return when (value) {
        is Int -> "$value"
        is Long -> "$value"
        else -> """ "$value" """
=======
private fun jsonToValue(value: Any, type: PropType): Any {
    return when (type) {
        PropType.GEOMETRY -> GeoJsonReader().read(value as String)
        else -> value
    }
}

fun edgesToAsterixCitizen(edges: List<R>, isUpdate: Boolean): String {
    return when (edges.size) {
        0 -> ""
        else -> """"$EDGES": [${edges.joinToString(",", transform = { e-> edgeToJson(e, isUpdate)})}],"""
    }
}

fun propertiesToAsterixCitizen(props: List<P>, isUpdate: Boolean): String =
    props.joinToString(",", transform = { p -> propToJson(p, isUpdate) }).let { if (it.isNotEmpty()) "$it," else "" }

fun edgeToJson(edge: R, isUpdate: Boolean): String {
    return """{"$LABEL": ${edge.label.ordinal},${propertiesToAsterixCitizen(edge.properties, isUpdate)}"$FROM_N": ${edge.fromN},"$TO_N": ${edge.toN}}"""
}

fun propToJson(property: P, isUpdate: Boolean): String {
    return if (property.key == LOCATION) {
        "\"${property.key}\": ${valueToJson(property.value, isUpdate)}"
    } else {
        """"${property.key}": {"$VALUE": ${valueToJson(property.value, isUpdate)},"$TYPE": ${property.type.ordinal}}"""
    }
}

fun jsonToProp(json: JSONObject, sourceId: Long, sourceType: Boolean, key: String, fromTimestamp: Long, toTimestamp: Long, g: Graph): P {
    val value: Any
    val type: PropType
    if (key == LOCATION) {
        type = PropType.GEOMETRY
        value = jsonToValue(json.toString(), type)
    } else {
        type = PropType.entries[json.getInt(TYPE)]
        value = jsonToValue(json.get(VALUE), type)
    }

    return P(id = DUMMY_ID, sourceId = sourceId, sourceType = sourceType, key = key, value = value, type = type, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)
}

fun jsonToEdge(json: JSONObject, fromTimestamp: Long, toTimestamp: Long, g: Graph): R {
    val id = DUMMY_ID
    val edge = R(id = id, label = Label.entries[json.getInt(LABEL)]!!, fromN = json.getLong(FROM_N), toN = json.getLong(TO_N), fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g = g)
    json.keys()
        .forEach { key ->
            if (!listOf(ID, LABEL, FROM_N, TO_N).contains(key)) {
                jsonToProp(json.getJSONObject(key), sourceId = id, sourceType = EDGE, key = key, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g)
            }
        }
    return edge
}

fun nodeToJson(n: N, isUpdate: Boolean, tsId: Long): String {
    return """{"$TSID": $tsId, "$ID": ${n.fromTimestamp}, "$LABEL": ${n.label.ordinal}, ${edgesToAsterixCitizen(n.edges, isUpdate)} ${propertiesToAsterixCitizen(n.properties, isUpdate)} "$VALUE": ${n.value ?: 0}}"""
}

fun jsonToNode(tsId: Long, g: Graph, node: JSONObject): N {
    val timestamp: Long
    val id: Long
    if (node.has(ID)) {
        timestamp = node.getLong(ID)
        id = encodeBitwise(tsId, timestamp)
    } else {
        id = DUMMY_ID
        timestamp = DUMMY_ID
    }

    val fromTimestamp = if (node.has(FROM_TIMESTAMP)) node.getLong(FROM_TIMESTAMP) else timestamp
    val toTimestamp = if (node.has(TO_TIMESTAMP)) node.getLong(TO_TIMESTAMP) else timestamp

    val entity = N(
        id = id,
        label = Label.entries[node.getInt(LABEL)]!!,
        fromTimestamp = fromTimestamp,
        toTimestamp = toTimestamp,
        value = if (id != DUMMY_ID) node.getLong(VALUE) else null,
        g = g
    )
    node.keys()
        .forEach { key ->
            if ((id == DUMMY_ID && key == VALUE) || !listOf(ID, LABEL, VALUE, FROM_TIMESTAMP, TO_TIMESTAMP, TSID).contains(key)) {
                when (key) {
                    EDGES -> node.getJSONArray(EDGES)
                        ?.let { array -> List(array.length()) { array.getJSONObject(it) } }
                        ?.map { jsonToEdge(it, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g) }
                        ?.let(entity.edges::addAll)
                    else -> {
                        jsonToProp(node.getJSONObject(key), sourceId = id, sourceType = NODE, key = key, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, g)
                            .let { entity.properties.add(it) }
                    }
                }
            }
        }
    return entity
}

fun valueToJson(value: Any, isUpdate: Boolean): String {
    return when (value) {
        is Int -> "$value"
        is Long -> "$value"
        is Double -> "$value"
        is Geometry -> if (isUpdate) "st_geom_from_text(\"${WKTWriter().write(value)}\")" else "\"${WKTWriter().write(value)}\""
        else -> "\"$value\""
        // else -> throw IllegalArgumentException("Unknown prop type ${value::class.qualifiedName}")
>>>>>>> feat-tssingletable
    }
}

fun applyFilters(filters: List<Filter>): String {
    return when {
        filters.isEmpty() -> ""
<<<<<<< HEAD
        else -> "WHERE ${filters.joinToString(separator = "\n AND ", transform = ::parseFilter)}"
=======
        else -> "WHERE ${filters.joinToString(separator = " AND ", transform = ::parseFilter)}"
>>>>>>> feat-tssingletable
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

<<<<<<< HEAD
    fun escapeIdentifier(name: String): String = if (name == "value") "`value`" else name

=======
>>>>>>> feat-tssingletable
    fun normalizeWkt(wkt: String): String? {
        val clean = wkt.trim().removeSurrounding("\"").removeSurrounding("'")
        val wktPattern = Regex(
            """^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(.*\)$""",
            RegexOption.IGNORE_CASE
        )
        return if (wktPattern.matches(clean)) clean else null
    }

    val property = filter.property
<<<<<<< HEAD
    val parsedValue = parseValue(filter.value)

    var left = if (filter.attrFirst) escapeIdentifier(property) else escapeIdentifier(parsedValue)
    var right = if (filter.attrFirst) escapeIdentifier(parsedValue) else escapeIdentifier(property)

    left = if (left == "fromTimestamp" || left == "toTimestamp") "timestamp" else left
    right = if (right == "fromTimestamp" || right == "toTimestamp") "timestamp" else right


    right = if(left == "timestamp" && right.toLong() < 0) "0" else right

=======
    val parsedValue = valueToJson(filter.value, isUpdate = false)
    val left = if (filter.attrFirst) property else parsedValue
    val right = if (filter.attrFirst) parsedValue else property
>>>>>>> feat-tssingletable
    return when (filter.operator) {
        Operators.ST_CONTAINS, Operators.ST_INTERSECTS -> {
            val arg1Raw = remove3DfromWkt(if (filter.attrFirst) right else left)
            val arg2Raw = remove3DfromWkt(if (filter.attrFirst) left else right)
<<<<<<< HEAD


            val arg1 = normalizeWkt(arg1Raw)?.let { "st_geom_from_text('$it')" } ?: arg1Raw
            val arg2 = normalizeWkt(arg2Raw)?.let { "st_geom_from_text('$it')" } ?: arg2Raw

            "${parseOperator(filter.operator)}($arg1, $arg2)"
        }

=======
            val arg1 = normalizeWkt(arg1Raw)?.let { "st_geom_from_text('$it')" } ?: arg1Raw
            val arg2 = normalizeWkt(arg2Raw)?.let { "st_geom_from_text('$it')" } ?: arg2Raw
            "${parseOperator(filter.operator)}($arg1, $arg2)"
        }
>>>>>>> feat-tssingletable
        else -> "$left ${parseOperator(filter.operator)} $right"
    }
}