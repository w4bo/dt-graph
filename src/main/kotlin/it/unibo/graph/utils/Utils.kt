package it.unibo.graph.utils

import it.unibo.graph.interfaces.Elem
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.PropType
import org.yaml.snakeyaml.Yaml
import java.io.*
import java.util.*

// Serialize an object to byte array
fun serialize(obj: Serializable): ByteArray {
    ByteArrayOutputStream().use { bos ->
        ObjectOutputStream(bos).use { out -> out.writeObject(obj) }
        return bos.toByteArray()
    }
}

// Deserialize a byte array to an object
inline fun <reified T : Elem> deserialize(bytes: ByteArray?, g: Graph): T {
    val r: T = ByteArrayInputStream(bytes).use { bis ->
        ObjectInputStream(bis).use { `in` -> `in`.readObject() as T }
    }
    (r as Elem).g = g
    return r
}

fun encodeBitwise(x: Long, y: Long, offset: Int = 44, mask: Long = 0xFFFFFFFFFFF): Long {
    return (x shl offset) or (y and mask)
}

fun decodeBitwise(z: Long, offset: Int = 44, mask: Long = 0xFFFFFFFFFFF): Pair<Long, Long> {
    val x = (z shr offset)
    val y = (z and mask)
    return Pair(x, y)
}

fun decodeBitwiseSource(z: Long, offset: Int = 44): Long {
    return z shr offset
}

fun <A, B> cartesianProduct(list1: List<A>, list2: List<B>): Set<Pair<A, B>> {
    return list1.flatMap { a -> list2.map { b -> a to b } }.toSet()
}

fun propTypeFromValue(value: Any): PropType {
    return when(value){
        is Int -> PropType.INT
        is Long -> PropType.LONG
        is Double -> PropType.DOUBLE
        is HashMap<*, *> -> {
            if (value.keys.contains(listOf("type", "coordinates"))) {
                PropType.GEOMETRY
            } else {
                PropType.STRING
            }
        }

        else -> PropType.STRING
    }
}

fun remove3DfromWkt(wkt: String): String {
    // 1. Rimuove eventuali marker Z, M, ZM
    var result = wkt.replace(Regex("""\bZ[M]?\b"""), "").trim()

    // 2. Cerca solo tuple con 3 o 4 numeri -> le riduce a 2
    result = result.replace(
        Regex("""(-?\d+(?:\.\d+)?)[ ]+(-?\d+(?:\.\d+)?)(?:[ ]+-?\d+(?:\.\d+)?){1,2}""")
    ) { match ->
        val x = match.groupValues[1]
        val y = match.groupValues[2]
        "$x $y"
    }

    return result
}

fun loadProps(): Properties {
    return Properties().apply {
        load(ClassLoader.getSystemResourceAsStream("config.properties"))
    }
}

typealias TemporalRanges = Map<Int, TimeRange>

data class TimeRange(
    val from: Long,
    val to: Long
)


// Funzione loadTemporalRanges che prende già la mappa YAML
fun loadTemporalRanges(
    yamlMap: Map<String, Any>,
    constraintType: String,
    queryName: String,
    datasetSize: String
): TemporalRanges {

    val temporalConstraints = yamlMap["temporalConstraints"] as? Map<*, *>
        ?: throw IllegalArgumentException("'temporalConstraints' not found in YAML")
    val constraintMap = temporalConstraints[constraintType] as? Map<*, *>
        ?: throw IllegalArgumentException("Constraint type '$constraintType' not found in YAML")
    val queryMap = constraintMap[queryName] as? Map<*, *>
        ?: throw IllegalArgumentException("Query '$queryName' not found under '$constraintType'")
    val datasetMap = queryMap[datasetSize] as? Map<*, *>
        ?: throw IllegalArgumentException("Dataset size '$datasetSize' not found under '$queryName'")

    return datasetMap.entries.associate { (k, v) ->
        val index = k.toString().toInt()
        val raw = v as List<*>
        index to TimeRange(
            from = raw[0].toString().toLong(),
            to = raw[1].toString().toLong()
        )
    }
}